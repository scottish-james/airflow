import os
import sys
import configparser
import logging
from pathlib import Path
from airflow.decorators import task

# Ensure Airflow recognizes dags directory for imports
sys.path.append('/opt/airflow')


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
def load_config(**context):
    """Load and validate configuration file."""
    try:
        config = configparser.ConfigParser()
        potential_paths = [
            Path('/opt/airflow/config/iac.cfg'),
            Path('/opt/airflow/dags/config/iac.cfg'),
            Path(os.getcwd()) / 'config/iac.cfg'
        ]

        # Check for existing config file
        config_path = next((path for path in potential_paths if path.exists()), None)

        if config_path:
            logger.info(f"Using existing config file: {config_path}")
        else:
            logger.warning("Config file not found. Creating default config.")
            config_path = Path('/opt/airflow/config/iac.cfg')
            config_path.parent.mkdir(parents=True, exist_ok=True)

            # Set default configuration
            config['AWS'] = {'region': 'us-west-2'}
            config['DWH'] = {
                'dwh_namespace': 'namespace_scottish_james',
                'dwh_workgroup': 'workgroup_scottish_james',
                'dwh_db': 'dwh',
                'dwh_db_user': 'awsuser',
                'dwh_db_password': 'Passw0rd',
                'dwh_base_capacity': '32'
            }

            # Save default config file
            with config_path.open('w') as f:
                config.write(f)
            logger.info(f"Default config created at: {config_path}")

        # Read config file
        config.read(config_path)
        config_dict = {
            section: {key.lower(): value for key, value in config.items(section)}
            for section in config.sections()
        }
        logger.info(f"Loaded config sections: {list(config_dict.keys())}")

        # Push config to XCom for other tasks to use
        context['task_instance'].xcom_push(key='config', value=config_dict)

        return config_dict

    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        raise