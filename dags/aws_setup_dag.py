from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Connection
from airflow.hooks.postgres_hook import PostgresHook
from airflow import settings
from datetime import datetime, timedelta
import boto3
import json
import configparser
import logging
import os
from pathlib import Path
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowNotFoundException

# Enhanced logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def load_config(**context):
    """Load and validate configuration file."""
    try:
        config = configparser.ConfigParser()

        # Try multiple potential locations in the container
        potential_paths = [
            '/opt/airflow/config/iac.cfg',  # Standard location in Airflow container
            '/opt/airflow/dags/config/iac.cfg',  # Under dags folder
            os.path.join(os.getcwd(), 'config/iac.cfg')  # Current working directory
        ]

        config_path = None
        for path in potential_paths:
            logger.info(f"Checking config path: {path}")
            if os.path.exists(path):
                logger.info(f"Found config at: {path}")
                config_path = Path(path)
                break

        if not config_path:
            # If config not found, create a default one for testing
            logger.warning("Config file not found in any expected location. Creating default config.")
            config_dir = '/opt/airflow/config'
            os.makedirs(config_dir, exist_ok=True)
            config_path = Path(f"{config_dir}/iac.cfg")

            # Create default config
            config['AWS'] = {
                'region': 'us-west-2'
            }
            config['DWH'] = {
                'dwh_namespace': 'namespace_scottish_james',
                'dwh_workgroup': 'workgroup_scottish_james',
                'dwh_db': 'dwh',
                'dwh_db_user': 'awsuser',
                'dwh_db_password': 'Passw0rd',
                'dwh_base_capacity': '32'
            }

            # Write default config
            with open(config_path, 'w') as f:
                config.write(f)

            logger.info(f"Created default config at {config_path}")

        # Check file permissions
        logger.info(f"File permissions: {oct(config_path.stat().st_mode)[-3:]}")

        # Try to read file with explicit encoding
        with open(config_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
            logger.debug(f"Config file content length: {len(file_content)}")

        config.read_string(file_content)  # Use read_string for better error handling
        if not config.sections():
            error_msg = "Configuration file is empty or malformed"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Convert to dictionary with lowercase keys
        config_dict = {section: {key.lower(): value for key, value in config.items(section)}
                       for section in config.sections()}

        # Log config sections (without sensitive data)
        logger.info(f"Found config sections: {list(config_dict.keys())}")
        logger.info(f"AWS Region: {config_dict.get('AWS', {}).get('region', 'not set')}")

        # Store the dictionary in XCom
        context['task_instance'].xcom_push(key='config', value=config_dict)
        return "Configuration loaded successfully"

    except Exception as e:
        logger.error(f"Error in load_config: {str(e)}")
        raise


def verify_aws_connection(**context):
    """Verify AWS credentials in Airflow from the aws_credentials connection."""
    try:
        # Get configuration
        config = context['task_instance'].xcom_pull(key='config')
        region = config.get('AWS', {}).get('region', 'us-west-2')

        # Check if aws_credentials connection exists
        try:
            conn = Connection.get_connection_from_secrets("aws_credentials")
            logger.info("Found aws_credentials connection")

            # Check if region matches config
            extra = conn.extra_dejson
            conn_region = extra.get('region_name')
            if conn_region and conn_region != region:
                logger.info(f"Connection region ({conn_region}) differs from config region ({region})")

            # Check if session token exists
            session_token = extra.get('aws_session_token')
            if session_token:
                logger.info("Session token is configured in connection")
            else:
                logger.warning("No session token found in connection")

            return "AWS connection verified"

        except AirflowNotFoundException:
            logger.error("aws_credentials connection not found in Airflow")
            raise ValueError("Please set up an 'aws_credentials' connection in Airflow UI")

    except Exception as e:
        logger.error(f"Error verifying AWS connection: {str(e)}")
        raise


def test_aws_connection(**context):
    """Test AWS connectivity before creating role."""
    try:
        # Get config from XCom
        config = context['task_instance'].xcom_pull(key='config')

        # Debug logging
        logger.info(f"Retrieved config from XCom: {config}")
        logger.info(f"AWS section: {config.get('AWS', {})}")

        # Initialize AWS Hook with aws_credentials connection
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='iam')
        credentials = aws_hook.get_credentials()

        # Log non-sensitive connection info
        logger.info(f"AWS Access Key ID ends with: ...{credentials.access_key[-4:]}")

        # Get region with fallback
        region = config.get('AWS', {}).get('region', 'us-west-2')
        logger.info(f"Using region: {region}")

        # Get session token from connection extra
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        session_token = extra.get('aws_session_token')
        logger.info("Session token is available" if session_token else "No session token found")

        # Create IAM client
        iam = boto3.client('iam',
                           aws_access_key_id=credentials.access_key,
                           aws_secret_access_key=credentials.secret_key,
                           aws_session_token=session_token,
                           region_name=region
                           )

        # Test listing roles
        response = iam.list_roles(MaxItems=1)
        logger.info(
            f"Successfully tested AWS IAM connection: {response['Roles'][0]['RoleName'] if response['Roles'] else 'No roles found'}")
        return "AWS connection test successful"

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"AWS Error - Code: {error_code}, Message: {error_message}")
        raise
    except Exception as e:
        logger.error(f"Error testing AWS connection: {str(e)}")
        raise


def create_iam_role(**context):
    """Create IAM role for Redshift Serverless with S3 access."""
    try:
        # SIMPLIFIED VERSION - HARDCODED VALUES
        # Initialize AWS hook and get credentials
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='iam')
        credentials = aws_hook.get_credentials()

        # Get config for region only
        config = context['task_instance'].xcom_pull(key='config')
        region = config.get('AWS', {}).get('region', 'us-west-2')

        # Get session token from connection
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        session_token = extra.get('aws_session_token')

        # Log important info
        logger.info(f"Using region: {region}")
        logger.info("Session token is available" if session_token else "No session token found")

        # Create IAM client
        iam = boto3.client('iam',
                           aws_access_key_id=credentials.access_key,
                           aws_secret_access_key=credentials.secret_key,
                           aws_session_token=session_token,
                           region_name=region
                           )

        # HARDCODED ROLE NAME - NO CONFIG DICTIONARY ACCESS
        role_name = "redshift-serverless-role-scottish-james"
        logger.info(f"Creating/updating role: {role_name}")

        # Clean up existing role if it exists
        try:
            existing_policies = iam.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']
            for policy in existing_policies:
                logger.info(f"Detaching policy: {policy['PolicyArn']}")
                iam.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])

            logger.info(f"Deleting existing role: {role_name}")
            iam.delete_role(RoleName=role_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                raise
            logger.info(f"Role {role_name} does not exist, proceeding with creation")

        # Create role - Updated for Redshift Serverless
        assume_role_policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "redshift-serverless.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        })

        logger.info(f"Creating new role: {role_name}")
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description='IAM role for Redshift Serverless with S3 access'
        )

        # Attach policies
        policies = [
            'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        ]

        for policy_arn in policies:
            logger.info(f"Attaching policy: {policy_arn}")
            try:
                iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
            except ClientError as e:
                logger.error(f"Error attaching policy {policy_arn}: {str(e)}")
                raise

        role_arn = role['Role']['Arn']
        logger.info(f"Successfully created role {role_name} with ARN: {role_arn}")

        # Store the role ARN in XCom
        context['task_instance'].xcom_push(key='iam_role_arn', value=role_arn)

        return f"IAM Role {role_name} created successfully with ARN: {role_arn}"

    except Exception as e:
        logger.error(f"Error in create_iam_role: {str(e)}")
        raise


def create_redshift_serverless(**context):
    """Create Redshift Serverless namespace and workgroup."""
    try:
        # Get config and role ARN
        config = context['task_instance'].xcom_pull(key='config')
        role_arn = context['task_instance'].xcom_pull(key='iam_role_arn')

        # Initialize AWS Hook
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='redshift-serverless')
        credentials = aws_hook.get_credentials()

        # Get session token from connection extra
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        session_token = extra.get('aws_session_token')

        # Get region with fallback
        region = config.get('AWS', {}).get('region', 'us-west-2')
        logger.info(f"Using region: {region}")
        logger.info(f"Using IAM role ARN: {role_arn}")

        # Create Redshift Serverless client
        redshift_serverless = boto3.client('redshift-serverless',
                                           aws_access_key_id=credentials.access_key,
                                           aws_secret_access_key=credentials.secret_key,
                                           aws_session_token=session_token,
                                           region_name=region
                                           )

        # Parse configs with defaults for missing values - ensure naming compliance with AWS
        # AWS requires namespace and workgroup names to follow pattern [a-z0-9-]+ (no underscores)
        namespace_name = config.get('DWH', {}).get('dwh_namespace', 'namespace-scottish-james').replace('_', '-')
        workgroup_name = config.get('DWH', {}).get('dwh_workgroup', 'workgroup-scottish-james').replace('_', '-')
        db_name = config.get('DWH', {}).get('dwh_db', 'dwh')
        db_user = config.get('DWH', {}).get('dwh_db_user', 'awsuser')
        db_password = config.get('DWH', {}).get('dwh_db_password', 'Passw0rd')
        base_capacity = int(config.get('DWH', {}).get('dwh_base_capacity', '32'))  # Default to 32 RPUs

        # Check if namespace already exists
        try:
            namespace_response = redshift_serverless.get_namespace(
                namespaceName=namespace_name
            )
            logger.info(f"Namespace {namespace_name} already exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Create namespace
                logger.info(f"Creating Redshift Serverless namespace: {namespace_name}")
                namespace_response = redshift_serverless.create_namespace(
                    namespaceName=namespace_name,
                    adminUsername=db_user,
                    adminUserPassword=db_password,
                    dbName=db_name,
                    iamRoles=[role_arn]
                )
                logger.info(f"Created namespace {namespace_name}")
            else:
                raise

        # Check if workgroup already exists
        try:
            workgroup_response = redshift_serverless.get_workgroup(
                workgroupName=workgroup_name
            )
            logger.info(f"Workgroup {workgroup_name} already exists")

            # Store endpoint
            endpoint = workgroup_response['workgroup']['endpoint']['address']
            port = workgroup_response['workgroup']['endpoint'].get('port', 5439)
            context['task_instance'].xcom_push(key='redshift_endpoint', value=endpoint)
            context['task_instance'].xcom_push(key='redshift_port', value=port)

            return f"Workgroup {workgroup_name} already exists with endpoint {endpoint}"
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Create workgroup
                logger.info(f"Creating Redshift Serverless workgroup: {workgroup_name}")
                workgroup_response = redshift_serverless.create_workgroup(
                    workgroupName=workgroup_name,
                    namespaceName=namespace_name,
                    baseCapacity=base_capacity,
                    publiclyAccessible=True
                )

                # Get endpoint information
                workgroup = redshift_serverless.get_workgroup(
                    workgroupName=workgroup_name
                )['workgroup']

                endpoint = workgroup['endpoint']['address']
                port = workgroup['endpoint'].get('port', 5439)

                # Store endpoint
                context['task_instance'].xcom_push(key='redshift_endpoint', value=endpoint)
                context['task_instance'].xcom_push(key='redshift_port', value=port)

                logger.info(f"Created workgroup {workgroup_name} with endpoint {endpoint}")
                return f"Created Redshift Serverless workgroup {workgroup_name} with endpoint {endpoint}"
            else:
                raise

    except Exception as e:
        logger.error(f"Error creating Redshift Serverless: {str(e)}")
        raise


def check_redshift_serverless_status(**context):
    """Check Redshift Serverless workgroup status."""
    try:
        # Get config
        config = context['task_instance'].xcom_pull(key='config')

        # Initialize AWS Hook
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='redshift-serverless')
        credentials = aws_hook.get_credentials()

        # Get session token from connection extra
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        session_token = extra.get('aws_session_token')

        # Get region with fallback
        region = config.get('AWS', {}).get('region', 'us-west-2')

        # Create Redshift Serverless client
        redshift_serverless = boto3.client('redshift-serverless',
                                           aws_access_key_id=credentials.access_key,
                                           aws_secret_access_key=credentials.secret_key,
                                           aws_session_token=session_token,
                                           region_name=region
                                           )

        # Get workgroup name with default - ensure AWS naming compliance
        workgroup_name = config.get('DWH', {}).get('dwh_workgroup', 'workgroup-scottish-james').replace('_', '-')

        # Check workgroup status
        workgroup = redshift_serverless.get_workgroup(
            workgroupName=workgroup_name
        )['workgroup']

        status = workgroup['status']

        if status == 'AVAILABLE':
            # Store endpoint information again to be safe
            endpoint = workgroup['endpoint']['address']
            port = workgroup['endpoint'].get('port', 5439)

            context['task_instance'].xcom_push(key='redshift_endpoint', value=endpoint)
            context['task_instance'].xcom_push(key='redshift_port', value=port)

            logger.info(f"Workgroup {workgroup_name} is available at {endpoint}")
            return f"Workgroup {workgroup_name} is available at {endpoint}"
        else:
            logger.info(f"Workgroup status: {status}. Waiting for workgroup to become available.")
            raise ValueError(f"Workgroup status is {status}, not 'AVAILABLE' yet")

    except Exception as e:
        logger.error(f"Error checking Redshift Serverless status: {str(e)}")
        raise


def setup_redshift_connection(**context):
    """Setup Airflow connection to Redshift Serverless."""
    try:
        config = context['task_instance'].xcom_pull(key='config')
        endpoint = context['task_instance'].xcom_pull(key='redshift_endpoint')
        port = context['task_instance'].xcom_pull(key='redshift_port', default=5439)

        if not endpoint:
            raise ValueError("Redshift endpoint not found")

        # Get Redshift connection details with defaults
        db_name = config.get('DWH', {}).get('dwh_db', 'dwh')
        db_user = config.get('DWH', {}).get('dwh_db_user', 'awsuser')
        db_password = config.get('DWH', {}).get('dwh_db_password', 'Passw0rd')

        # Create connection object
        conn = Connection(
            conn_id='redshift',
            conn_type='postgres',
            host=endpoint,
            login=db_user,
            password=db_password,
            schema=db_name,
            port=port
        )

        # Get session
        session = settings.Session()

        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

        if existing_conn:
            session.delete(existing_conn)
            session.commit()

        # Add new connection
        session.add(conn)
        session.commit()
        session.close()

        logger.info(f"Created Redshift connection: redshift")
        return "Redshift connection created"

    except Exception as e:
        logger.error(f"Error setting up Redshift connection: {str(e)}")
        raise


def create_tables(**context):
    """Create tables in Redshift Serverless."""
    try:
        # Get Redshift hook
        redshift = PostgresHook(postgres_conn_id='redshift')

        # Read SQL from create_tables.sql
        sql_path = '/opt/airflow/create_tables.sql'

        with open(sql_path, 'r') as f:
            create_tables_sql = f.read()

        # Execute create tables SQL
        redshift.run(create_tables_sql)

        logger.info("Created tables in Redshift Serverless")
        return "Tables created successfully"

    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise


# Create DAG
dag = DAG(
    'aws_setup_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 2, 16),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description='Set up AWS resources and Redshift Serverless for ETL pipeline',
    schedule_interval=None,
    catchup=False
)

# Define tasks
load_config_task = PythonOperator(
    task_id='load_config',
    python_callable=load_config,
    dag=dag
)

verify_aws_task = PythonOperator(
    task_id='verify_aws_connection',
    python_callable=verify_aws_connection,
    dag=dag
)

test_aws_task = PythonOperator(
    task_id='test_aws_connection',
    python_callable=test_aws_connection,
    dag=dag
)

create_role_task = PythonOperator(
    task_id='create_iam_role',
    python_callable=create_iam_role,
    dag=dag
)

create_redshift_task = PythonOperator(
    task_id='create_redshift_serverless',
    python_callable=create_redshift_serverless,
    dag=dag
)

check_redshift_task = PythonOperator(
    task_id='check_redshift_serverless_status',
    python_callable=check_redshift_serverless_status,
    retries=10,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

setup_connection_task = PythonOperator(
    task_id='setup_redshift_connection',
    python_callable=setup_redshift_connection,
    dag=dag
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag
)

# Set dependencies
load_config_task >> verify_aws_task >> test_aws_task >> create_role_task >> create_redshift_task >> check_redshift_task >> setup_connection_task >> create_tables_task