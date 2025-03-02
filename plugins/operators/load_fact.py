from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Custom Airflow Operator to load data into a Redshift fact table.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str = "redshift",
        sql_query: str = "",
        target_table: str = "",
        *args, **kwargs
    ):
        """
        Initialize the LoadFactOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param sql_query: SQL query to insert data into the fact table
        :param target_table: Target Redshift fact table
        """
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.target_table = target_table

    def execute(self, context):
        """
        Execute the task: Load data into the fact table.
        """
        self.log.info("LoadFactOperator is starting...")

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading data into fact table {self.target_table}")

        # Construct and execute the INSERT query
        insert_sql = f"INSERT INTO {self.target_table} {self.sql_query}"
        redshift.run(insert_sql)

        self.log.info(f"Successfully loaded data into fact table {self.target_table}")