from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime, timedelta
import boto3
import json
import configparser
import logging
from pathlib import Path
from botocore.exceptions import ClientError

# Enhanced logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_config(**context):
    """Load and validate configuration file."""
    try:
        config = configparser.ConfigParser()
        config_path = Path('/opt/airflow/config/iac.cfg')

        if not config_path.exists():
            error_msg = f"Configuration file not found at {config_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        # Check file permissions
        logger.info(f"File permissions: {oct(config_path.stat().st_mode)[-3:]}")

        # Try to read file
        with open(config_path, 'r') as f:
            file_content = f.read()
            logger.debug(f"Config file content length: {len(file_content)}")

        config.read(config_path)
        if not config.sections():
            error_msg = "Configuration file is empty"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Convert ConfigParser to dictionary with lowercase keys
        config_dict = {}
        for section in config.sections():
            config_dict[section] = {}
            for key, value in config.items(section):
                config_dict[section][key.lower()] = value

        # Log config sections (without sensitive data)
        logger.info(f"Found config sections: {list(config_dict.keys())}")
        logger.info(f"AWS Region: {config_dict.get('AWS', {}).get('region', 'not set')}")

        # Store the dictionary in XCom
        context['task_instance'].xcom_push(key='config', value=config_dict)
        return "Configuration loaded successfully"

    except Exception as e:
        logger.error(f"Error in load_config: {str(e)}")
        raise


def test_aws_connection(**context):
    """Test AWS connectivity before creating role."""
    try:
        # Get config from XCom
        config = context['task_instance'].xcom_pull(key='config')

        # Debug logging
        logger.info(f"Retrieved config from XCom: {config}")
        logger.info(f"AWS section: {config.get('AWS', {})}")

        # Initialize AWS Hook
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type='iam')
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

        # Test IAM access with session token
        iam = boto3.client('iam',
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            aws_session_token=session_token,
            region_name=region
        )

        # Test listing roles
        iam.list_roles(MaxItems=1)
        logger.info("Successfully tested AWS IAM connection")
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
    """Create IAM role with enhanced error handling."""
    try:
        config = context['task_instance'].xcom_pull(key='config')
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type='iam')
        credentials = aws_hook.get_credentials()

        # Get region with fallback
        region = config.get('AWS', {}).get('region', 'us-west-2')
        logger.info(f"Using region: {region}")

        # Get session token from connection extra
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        session_token = extra.get('aws_session_token')
        logger.info("Session token is available" if session_token else "No session token found")

        iam = boto3.client('iam',
           aws_access_key_id=credentials.access_key,
           aws_secret_access_key=credentials.secret_key,
           aws_session_token=session_token,
           region_name=region
        )

        # Get role name from config (using lowercase keys)
        role_name = config['DWH']['dwh_iam_role_name']
        logger.info(f"Creating/updating role: {role_name}")

        # Clean up existing role
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

        # Create role
        assume_role_policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        })

        logger.info(f"Creating new role: {role_name}")
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description='IAM role for Redshift with full access'
        )

        # Attach policies
        policies = [
            'arn:aws:iam::aws:policy/AdministratorAccess',
            'arn:aws:iam::aws:policy/AmazonRedshiftFullAccess',
            'arn:aws:iam::aws:policy/AmazonS3FullAccess'
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
        return f"IAM Role {role_name} created successfully with ARN: {role_arn}"

    except Exception as e:
        logger.error(f"Error in create_iam_role: {str(e)}")
        raise


# Create DAG
dag = DAG(
    'create_iam_role_debug',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 2, 16),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Create IAM role for Redshift with debug logging',
    schedule_interval=None,
    catchup=False
)

# Define tasks
load_config_task = PythonOperator(
    task_id='load_config',
    python_callable=load_config,
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

# Set dependencies
load_config_task >> test_aws_task >> create_role_task