from airflow.decorators import task
from airflow.models import Connection
from airflow.hooks.postgres_hook import PostgresHook
from airflow import settings
import os
import logging
import sys


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
def setup_redshift_connection(**context):
    """Set up Airflow connection to Redshift Serverless."""
    try:
        # Get config from XCom
        config = context['task_instance'].xcom_pull(key='config')

        endpoint = config.get('DWH', {}).get('redshift_endpoint')
        port = config.get('DWH', {}).get('redshift_port', 5439)
        db_name = config.get('DWH', {}).get('dwh_db', 'dwh')
        db_user = config.get('DWH', {}).get('dwh_db_user', 'awsuser')
        db_password = config.get('DWH', {}).get('dwh_db_password', 'Passw0rd')

        if not endpoint:
            raise ValueError("Redshift endpoint not found in config")

        logger.info(f"Setting up Redshift connection to {endpoint}:{port}/{db_name}")

        conn = Connection(
            conn_id='redshift',
            conn_type='postgres',
            host=endpoint,
            login=db_user,
            password=db_password,
            schema=db_name,
            port=port
        )

        session = settings.Session()
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

        if existing_conn:
            logger.info(f"Deleting existing Redshift connection: {conn.conn_id}")
            session.delete(existing_conn)
            session.commit()

        session.add(conn)
        session.commit()
        session.close()

        logger.info("Created Redshift connection in Airflow")
        return "Redshift connection created"
    except Exception as e:
        logger.error(f"Error setting up Redshift connection: {str(e)}")
        raise


@task()
def create_tables():
    """Create tables in Redshift Serverless."""
    try:
        redshift = PostgresHook(postgres_conn_id='redshift')
        sql_path = '/opt/airflow/sql/create_tables.sql'

        if not os.path.exists(sql_path):
            logger.warning(f"SQL file not found: {sql_path}")
            # Create a simple test table if the file doesn't exist
            create_tables_sql = """
            CREATE TABLE IF NOT EXISTS test_table (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT GETDATE()
            );
            """
            logger.info("Using default test table creation SQL")
        else:
            with open(sql_path, 'r') as f:
                create_tables_sql = f.read()
                logger.info(f"Loaded SQL from {sql_path}")

        redshift.run(create_tables_sql)
        logger.info("Created tables in Redshift Serverless")
        return "Tables created successfully"
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise