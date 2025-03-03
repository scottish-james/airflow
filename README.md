# Data Pipelines with Apache Airflow

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process music streaming data from an S3 bucket into an analytics-ready data warehouse in Amazon Redshift. The pipeline enables easy analysis of user activity for a fictional music streaming service.

## Project Overview

This data pipeline:
1. Extracts song and user activity data from S3
2. Stages the raw data in Redshift
3. Transforms the data into a star schema optimised for analytical queries
4. Performs data quality checks to ensure data integrity

The project showcases best practices in data engineering, including custom Airflow operators, modular pipeline design, and automated data validation.

## Data Model

The data is modeled using a star schema with:

- **Fact Table**: `songplays` - Records of user song plays
- **Dimension Tables**:
  - `users` - User information
  - `songs` - Song information
  - `artists` - Artist information
  - `time` - Timestamps broken down into specific units

## Project Structure

```
.
├── dags/                     # Airflow DAG definitions
│   ├── aws_setup_dag.py      # DAG for setting up AWS resources (legacy version)
│   ├── aws_setup_dag_v2.py   # Modular DAG for setting up AWS resources
│   ├── final_project.py      # Main ETL pipeline DAG
│   └── tasks/                # Modular task definitions
│       ├── __init__.py       # Package initialization
│       ├── aws.py            # AWS-related tasks
│       ├── config.py         # Configuration loading tasks
│       ├── database.py       # Database setup tasks
│       └── redshift.py       # Redshift setup tasks
├── plugins/
│   ├── helpers/              # Helper modules
│   │   ├── __init__.py       # Package initialization
│   │   └── sql_queries.py    # SQL transformation queries
│   └── operators/            # Custom Airflow operators
│       ├── __init__.py       # Package initialization
│       ├── data_quality.py   # Data quality checking operator
│       ├── load_dimension.py # Dimension table loading operator
│       ├── load_fact.py      # Fact table loading operator
│       └── stage_redshift.py # S3 to Redshift staging operator
├── config/                   # Configuration files
│   └── iac.cfg               # AWS configurations
├── create_tables.sql         # SQL for creating tables in Redshift
├── docker-compose.yaml       # Docker configuration for Airflow
├── README.md                 # Project documentation
└── LICENSE.md                # License information
```

## Custom Operators

This project implements four custom operators:

1. **StageToRedshiftOperator**: Loads JSON-formatted files from S3 to Redshift
2. **LoadFactOperator**: Transforms staged data and loads it into the fact table
3. **LoadDimensionOperator**: Transforms staged data and loads it into dimension tables
4. **DataQualityOperator**: Runs data quality checks on the loaded data

## Pipeline Workflow

The pipeline follows this workflow:
1. Verify AWS credentials and S3 connectivity
2. Stage event and song data from S3 to Redshift
3. Transform staged data and load into the songplays fact table
4. Transform staged data and load into dimension tables (users, songs, artists, time)
5. Run data quality checks to validate the data

![airflow_dag.png](assets%2Fairflow_dag.png)

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- AWS account with appropriate permissions
- Redshift Serverless namespace and workgroup (or can be created with the setup DAG)

### Running the Project

1. Clone the repository
2. Create an `.env` file for environment variables (optional)
3. Start the Airflow services:

```bash
docker-compose up -d
```

4. Access the Airflow web UI at http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`

5. Configure connections in Airflow:
   - Navigate to Admin > Connections
   - Add an `aws_credentials` connection with your AWS access key, secret key, and session token
   - Add a `redshift` connection with your Redshift serverless endpoint details

6. Run the setup DAG first to create AWS resources (if needed):
   - Trigger the `aws_setup_dag_v2` (recommended) or `aws_setup_dag`

7. Once setup is complete, trigger the main ETL pipeline:
   - Trigger the `final_project` DAG

## AWS Setup DAG

The project includes two versions of the AWS setup DAG:

### Legacy Version (`aws_setup_dag.py`)
This is the original monolithic implementation that sets up all AWS resources in a single file.

### Modular Version (`aws_setup_dag_v2.py`) - Recommended
This new modular implementation breaks down tasks into reusable components in the `dags/tasks/` directory for better maintainability and readability.

Both DAGs automate the AWS infrastructure provisioning process:

1. **Load Configuration** - Reads the AWS and Redshift configuration from the `config/iac.cfg` file
2. **Verify AWS Connection** - Validates that the AWS credentials are properly configured in Airflow
3. **Test AWS Connection** - Tests connectivity to AWS services
4. **Create IAM Role** - Creates an IAM role for Redshift with appropriate permissions to access S3
5. **Create Redshift Serverless** - Provisions a Redshift Serverless namespace and workgroup
6. **Check Redshift Status** - Waits for the Redshift resources to become available
7. **Setup Redshift Connection** - Configures the Airflow connection to Redshift
8. **Create Tables** - Creates the necessary staging and analytics tables in Redshift

### Configuration Parameters

The AWS setup DAGs use the following configuration parameters from `config/iac.cfg`:

```ini
[AWS]
REGION=us-west-2

[DWH]
dwh_namespace=namespace-scottish-james
dwh_workgroup=workgroup-scottish-james
dwh_base_capacity=32
dwh_db=dwh
dwh_db_user=awsuser
dwh_db_password=Passw0rd

[S3]
LOG_DATA=s3://udacity-dend/log_data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song_data
```

## ETL Pipeline DAG (`final_project.py`)

The ETL pipeline DAG (`final_project.py`) orchestrates the data flow from S3 to Redshift:

1. **Verify AWS Credentials** - Ensures AWS connectivity
2. **Test S3 Connection** - Validates access to the source data
3. **Stage Data** - Copies raw data from S3 to Redshift staging tables
4. **Load Fact Table** - Transforms and loads data into the songplays fact table
5. **Load Dimension Tables** - Transforms and loads data into dimension tables
6. **Run Data Quality Checks** - Validates the integrity of the loaded data

## Data Sources

This project uses two datasets stored in S3:

1. **Song Data**: JSON files containing song metadata
   - Location: `s3://udacity-dend/song_data`
   - Example fields: song_id, title, artist_id, year, duration

2. **Log Data**: JSON files containing user activity logs
   - Location: `s3://udacity-dend/log_data`
   - Example fields: userId, firstName, lastName, gender, level, location, userAgent

## Customisation

- Modify `sql_queries.py` to change the transformation logic
- Adjust the DAG schedule in `final_project.py` to change the execution frequency
- Update the data quality checks in the DAG definition to implement additional validation rules

## Troubleshooting

If you encounter any issues:

1. Check the Airflow task logs for detailed error messages
2. Verify your AWS credentials and permissions
3. Ensure your Redshift Serverless instance is running
4. Confirm that the S3 buckets are accessible

## Important Notes

- Redshift Serverless incurs costs as long as it's running (~$0.5-1 per hour depending on configuration)
- Remember to clean up AWS resources when they're no longer needed to avoid unnecessary charges

## Acknowledgments

- This project was developed as part of the Udacity Data Engineering Nanodegree
- The dataset is provided by [Million Song Dataset](http://millionsongdataset.com/) and user activity simulation
