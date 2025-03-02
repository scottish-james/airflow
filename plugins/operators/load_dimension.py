from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Custom Airflow Operator to load data into a Redshift dimension table.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str = "redshift",
        sql_query: str = "",
        target_table: str = "",
        truncate_table: bool = True,
        *args, **kwargs
    ):
        """
        Initialize the LoadDimensionOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param sql_query: SQL query to insert data into the dimension table
        :param target_table: Target Redshift dimension table
        :param truncate_table: Whether to truncate the table before insertion
        """
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.target_table = target_table
        self.truncate_table = truncate_table

    def execute(self, context):
        """
        Execute the task: Load data into the dimension table.
        Supports optional truncation before insertion.
        """
        self.log.info("LoadDimensionOperator is starting...")

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate table if specified
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.target_table}")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")

        self.log.info(f"Loading data into dimension table {self.target_table}")

        # Construct and execute the INSERT query
        insert_sql = f"INSERT INTO {self.target_table} {self.sql_query}"
        redshift.run(insert_sql)

        self.log.info(
            f"Successfully loaded data into dimension table {self.target_table}"
        )