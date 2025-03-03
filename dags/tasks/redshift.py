import boto3
from botocore.exceptions import ClientError
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import logging
import sys


# Create a simple logger function
def get_logger():
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


logger = get_logger()


@task()
def create_redshift_serverless(**context):
    """Create Redshift Serverless namespace and workgroup."""
    try:
        # Get config and role_arn from XCom
        config = context['task_instance'].xcom_pull(key='config')
        role_arn = context['task_instance'].xcom_pull(key='iam_role_arn')

        if not role_arn:
            raise ValueError(
                "IAM Role ARN not found in XCom. Please ensure the create_iam_role task completed successfully.")

        # Get AWS credentials with session token
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='redshift-serverless')
        credentials = aws_hook.get_credentials()

        # Get session token from connection
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        session_token = extra.get('aws_session_token')

        region = config.get('AWS', {}).get('region', 'us-west-2')

        # Initialize Redshift Serverless client with session token
        if session_token:
            redshift = boto3.client('redshift-serverless',
                                    aws_access_key_id=credentials.access_key,
                                    aws_secret_access_key=credentials.secret_key,
                                    aws_session_token=session_token,
                                    region_name=region)
        else:
            redshift = boto3.client('redshift-serverless',
                                    aws_access_key_id=credentials.access_key,
                                    aws_secret_access_key=credentials.secret_key,
                                    region_name=region)

        namespace_name = config.get('DWH', {}).get('dwh_namespace', 'namespace-scottish-james').replace('_', '-')
        workgroup_name = config.get('DWH', {}).get('dwh_workgroup', 'workgroup-scottish-james').replace('_', '-')

        # Try to get existing namespace first
        namespace_exists = False
        try:
            namespace = redshift.get_namespace(namespaceName=namespace_name)
            namespace_exists = True
            logger.info(f"Namespace {namespace_name} already exists")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise
            logger.info(f"Namespace {namespace_name} does not exist, will create it")

        # Create namespace if it doesn't exist
        if not namespace_exists:
            try:
                redshift.create_namespace(
                    namespaceName=namespace_name,
                    adminUsername=config['DWH']['dwh_db_user'],
                    adminUserPassword=config['DWH']['dwh_db_password'],
                    dbName=config['DWH']['dwh_db'],
                    iamRoles=[role_arn]
                )
                logger.info(f"Created Redshift namespace: {namespace_name}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConflictException':
                    raise
                logger.info(f"Namespace {namespace_name} already exists (conflict detected)")

        # Check if the namespace has our IAM role
        try:
            namespace = redshift.get_namespace(namespaceName=namespace_name)
            existing_roles = namespace['namespace'].get('iamRoles', [])
            role_exists = any(r == role_arn for r in existing_roles)

            if not role_exists:
                logger.info(f"Adding IAM role {role_arn} to namespace {namespace_name}")
                all_roles = existing_roles + [role_arn]
                redshift.update_namespace(
                    namespaceName=namespace_name,
                    iamRoles=all_roles
                )
        except Exception as e:
            logger.warning(f"Could not update namespace IAM roles: {str(e)}")

        # Check if workgroup exists
        workgroup_exists = False
        try:
            workgroup = redshift.get_workgroup(workgroupName=workgroup_name)
            workgroup_exists = True
            logger.info(f"Workgroup {workgroup_name} already exists")
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise
            logger.info(f"Workgroup {workgroup_name} does not exist, will create it")

        # Create workgroup if it doesn't exist
        if not workgroup_exists:
            try:
                redshift.create_workgroup(
                    workgroupName=workgroup_name,
                    namespaceName=namespace_name,
                    baseCapacity=int(config['DWH']['dwh_base_capacity']),
                    publiclyAccessible=True
                )
                logger.info(f"Created Redshift workgroup: {workgroup_name}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConflictException':
                    raise
                logger.info(f"Workgroup {workgroup_name} already exists (conflict detected)")

        # Store endpoint information in XCom for later tasks
        try:
            workgroup = redshift.get_workgroup(workgroupName=workgroup_name)['workgroup']
            endpoint = workgroup.get('endpoint', {}).get('address')
            port = workgroup.get('endpoint', {}).get('port', 5439)

            # Store endpoint in config and push to XCom
            if not config.get('DWH'):
                config['DWH'] = {}
            config['DWH']['redshift_endpoint'] = endpoint
            config['DWH']['redshift_port'] = port
            context['task_instance'].xcom_push(key='config', value=config)

            logger.info(f"Redshift endpoint: {endpoint}:{port}")
        except Exception as e:
            logger.warning(f"Could not retrieve endpoint information: {str(e)}")

        return f"Redshift Serverless {namespace_name} and {workgroup_name} configured"
    except Exception as e:
        logger.error(f"Error creating Redshift Serverless: {str(e)}")
        raise


@task()
def check_redshift_serverless_status(**context):
    """Check Redshift Serverless status."""
    try:
        # Get config from XCom
        config = context['task_instance'].xcom_pull(key='config')

        # Get AWS credentials with session token
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='redshift-serverless')
        credentials = aws_hook.get_credentials()

        # Get session token from connection
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        session_token = extra.get('aws_session_token')

        region = config.get('AWS', {}).get('region', 'us-west-2')

        # Initialize Redshift Serverless client with session token
        redshift = boto3.client('redshift-serverless',
                                aws_access_key_id=credentials.access_key,
                                aws_secret_access_key=credentials.secret_key,
                                aws_session_token=session_token,
                                region_name=region)

        workgroup_name = config.get('DWH', {}).get('dwh_workgroup', 'workgroup-scottish-james').replace('_', '-')
        workgroup = redshift.get_workgroup(workgroupName=workgroup_name)['workgroup']
        status = workgroup['status']

        logger.info(f"Workgroup {workgroup_name} status: {status}")

        if status == 'AVAILABLE':
            # Store endpoint information in XCom for later tasks
            endpoint = workgroup.get('endpoint', {}).get('address')
            port = workgroup.get('endpoint', {}).get('port', 5439)

            if not config.get('DWH'):
                config['DWH'] = {}
            config['DWH']['redshift_endpoint'] = endpoint
            config['DWH']['redshift_port'] = port
            context['task_instance'].xcom_push(key='config', value=config)

            logger.info(f"Redshift endpoint: {endpoint}:{port}")
            return f"Workgroup {workgroup_name} is available at {endpoint}:{port}"
        else:
            raise ValueError(f"Workgroup status is {status}, not 'AVAILABLE' yet")
    except Exception as e:
        logger.error(f"Error checking Redshift status: {str(e)}")
        raise