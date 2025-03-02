from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Custom Airflow Operator to copy data from S3 to Redshift staging tables.
    """
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
    def __init__(
        self,
        redshift_conn_id: str = "redshift",
        aws_credentials_id: str = "aws_credentials",
        table: str = "",
        s3_bucket: str = "",
        s3_key: str = "",
        json_path: str = "auto",
        region: str = "us-west-2",
        *args, **kwargs
    ):
        """
        Initialize the StageToRedshiftOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param aws_credentials_id: Airflow connection ID for AWS credentials
        :param table: Target Redshift table
        :param s3_bucket: S3 bucket name
        :param s3_key: S3 object key (supports templating)
        :param json_path: JSON format path (default: 'auto')
        :param region: AWS region (default: 'us-west-2')
        """
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        """
        Execute the task: Copy data from S3 to Redshift staging table.
        """
        self.log.info("StageToRedshiftOperator is starting...")

        # Get AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data in the target table
        self.log.info(f"Clearing data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Render S3 key with execution context
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        # Prepare COPY SQL command
        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            credentials.token,  # Ensures session token is included
            self.json_path,
            self.region
        )

        # Execute COPY command in Redshift
        redshift.run(formatted_sql)
        self.log.info(f"Successfully copied data to {self.table}")