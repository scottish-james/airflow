from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_query="",
                 target_table="",
                 truncate_table=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.target_table = target_table
        self.truncate_table = truncate_table

    def execute(self, context):
        """
        Insert data into dimension table using the provided SQL query
        with option to truncate the table before insertion
        """
        self.log.info('LoadDimensionOperator is starting')

        # Create Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check if truncate operation is requested
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.target_table}")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")

        self.log.info(f"Loading data into dimension table {self.target_table}")

        # Use the provided SQL query to insert data
        insert_sql = f"INSERT INTO {self.target_table} {self.sql_query}"
        redshift.run(insert_sql)

        self.log.info(f"Successfully loaded data into dimension table {self.target_table}")