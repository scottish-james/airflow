from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_query="",
                 target_table="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.target_table = target_table

    def execute(self, context):
        """
        Insert data into fact table using the provided SQL query
        """
        self.log.info('LoadFactOperator is starting')

        # Create Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading data into fact table {self.target_table}")

        # Use the provided SQL query to insert data
        insert_sql = f"INSERT INTO {self.target_table} {self.sql_query}"
        redshift.run(insert_sql)

        self.log.info(f"Successfully loaded data into fact table {self.target_table}")