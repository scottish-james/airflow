from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        SESSION_TOKEN '{}'
        JSON '{}'
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 region="us-west-2",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Save all the parameters as instance variables
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        """
        Copy data from S3 to Redshift staging tables
        using the provided parameters
        """
        self.log.info('StageToRedshiftOperator is starting')

        # AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear the staging table
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Render the S3 key with the execution date if needed
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        # Prepare the COPY command
        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            credentials.token,  # Add this line for the session token
            self.json_path,
            self.region
        )

        # Execute the COPY command
        redshift.run(formatted_sql)
        self.log.info(f"Successfully copied data to {self.table}")