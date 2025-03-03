from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Connection
from airflow.exceptions import AirflowNotFoundException
from airflow.decorators import task
import logging
import sys
import json
import boto3
from botocore.exceptions import ClientError

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
def verify_aws_connection(config):
    """Verify AWS credentials from the airflow connection."""
    try:
        region = config.get('AWS', {}).get('region', 'us-west-2')
        try:
            conn = Connection.get_connection_from_secrets("aws_credentials")
            extra = conn.extra_dejson
            conn_region = extra.get('region_name', region)
            logger.info(f"AWS Connection found. Region: {conn_region}")
            return "AWS connection verified"
        except AirflowNotFoundException:
            raise ValueError("AWS credentials connection not found. Set up 'aws_credentials' in Airflow UI.")
    except Exception as e:
        logger.error(f"Error verifying AWS connection: {str(e)}")
        raise


@task()
def test_aws_connection(config):
    """Test AWS connectivity before proceeding."""
    try:
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='iam')
        credentials = aws_hook.get_credentials()
        logger.info(f"AWS Access Key ID ends with: ...{credentials.access_key[-4:]}")
        return "AWS connection test successful"
    except Exception as e:
        logger.error(f"Error testing AWS connection: {str(e)}")
        raise


@task()
def create_iam_role(**context):
    """Create IAM role for Redshift Serverless with S3 access."""
    try:
        # Initialize AWS hook and get credentials
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='iam')
        credentials = aws_hook.get_credentials()

        # Get config from the context
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