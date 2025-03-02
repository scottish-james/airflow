from datetime import datetime, timedelta
import pendulum
import os
import boto3
import json
import logging
from botocore.exceptions import ClientError
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# Setup logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'james_taylor',
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),  # Fixed start_date
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}


def verify_aws_connection(**context):
    """Verify AWS credentials in Airflow from the aws_credentials connection."""
    try:
        # Check if aws_credentials connection exists
        try:
            conn = Connection.get_connection_from_secrets("aws_credentials")
            logger.info("Found aws_credentials connection")

            # Check if region is configured
            extra = conn.extra_dejson
            conn_region = extra.get('region_name')
            if conn_region:
                logger.info(f"Connection region: {conn_region}")
            else:
                logger.warning("No region found in connection")

            # Check if session token exists
            session_token = extra.get('aws_session_token')
            if session_token:
                logger.info("Session token is configured in connection")
                # Log a portion of the token for verification
                token_preview = session_token[:10] + "..." + session_token[-10:]
                logger.info(f"Session token preview: {token_preview}")
            else:
                logger.warning("No session token found in connection")

            # Log access key info
            if conn.login:
                logger.info(f"AWS Access Key ID ends with: ...{conn.login[-4:]}")
            else:
                logger.warning("No AWS Access Key ID found")

            return "AWS connection verified"

        except AirflowNotFoundException:
            logger.error("aws_credentials connection not found in Airflow")
            raise ValueError("Please set up an 'aws_credentials' connection in Airflow UI")

    except Exception as e:
        logger.error(f"Error verifying AWS connection: {str(e)}")
        raise


def test_aws_s3_connection(**context):
    """Test AWS S3 connectivity to make sure we can access the data source."""
    try:
        # Initialize AWS Hook with aws_credentials connection
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', client_type='s3')
        credentials = aws_hook.get_credentials()

        # Log non-sensitive connection info
        logger.info(f"AWS Access Key ID ends with: ...{credentials.access_key[-4:]}")

        # Get session token from connection extra
        conn = aws_hook.get_connection(aws_hook.aws_conn_id)
        extra = conn.extra_dejson
        region = extra.get('region_name', 'us-west-2')
        logger.info(f"Using region: {region}")
        session_token = extra.get('aws_session_token')
        logger.info("Session token is available" if session_token else "No session token found")

        # Create S3 client
        s3 = boto3.client('s3',
                          aws_access_key_id=credentials.access_key,
                          aws_secret_access_key=credentials.secret_key,
                          aws_session_token=session_token,
                          region_name=region
                          )

        # Test listing objects
        bucket = 'udacity-dend'
        prefix = 'log_data'
        logger.info(f"Testing S3 access to s3://{bucket}/{prefix}")

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)

        if 'Contents' in response and response['Contents']:
            obj_key = response['Contents'][0]['Key']
            logger.info(f"Successfully accessed S3 bucket. Found object: {obj_key}")
        else:
            logger.warning(f"Successfully accessed S3 bucket but no objects found with prefix {prefix}")

        # Test access to log_json_path.json
        try:
            json_path_key = 'log_json_path.json'
            logger.info(f"Testing access to s3://{bucket}/{json_path_key}")
            response = s3.head_object(Bucket=bucket, Key=json_path_key)
            logger.info(f"Successfully verified log_json_path.json exists (size: {response['ContentLength']} bytes)")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"log_json_path.json NOT FOUND in bucket {bucket}")
            else:
                raise

        return "AWS S3 connection test successful"

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"AWS S3 Error - Code: {error_code}, Message: {error_message}")
        raise
    except Exception as e:
        logger.error(f"Error testing AWS S3 connection: {str(e)}")
        raise


@dag(
    dag_id="final_project",
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():
    start_operator = EmptyOperator(task_id='Begin_execution')

    # Add AWS verification tasks
    verify_aws_creds = PythonOperator(
        task_id='Verify_aws_credentials',
        python_callable=verify_aws_connection
    )

    test_s3_connection = PythonOperator(
        task_id='Test_s3_connection',
        python_callable=test_aws_s3_connection
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key='log_data',
        json_path='s3://udacity-dend/log_json_path.json',
        region='us-west-2'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        json_path='auto',
        region='us-west-2'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.songplay_table_insert,
        target_table='songplays'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.user_table_insert,
        target_table='users',
        truncate_table=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.song_table_insert,
        target_table='songs',
        truncate_table=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.artist_table_insert,
        target_table='artists',
        truncate_table=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.time_table_insert,
        target_table='time',
        truncate_table=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        test_cases=[
            {'sql': 'SELECT COUNT(*) FROM songplays', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM users', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM songs', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM artists', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM time', 'expected_result': 0, 'operator': '>'}
        ]
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    # Set dependencies with verification tasks
    start_operator >> verify_aws_creds >> test_s3_connection

    # Connect verification to staging
    test_s3_connection >> stage_events_to_redshift >> load_songplays_table
    test_s3_connection >> stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()