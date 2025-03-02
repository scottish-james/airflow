# Data Pipelines with Apache Airflow

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process music streaming data from an S3 bucket into an analytics-ready data warehouse in Amazon Redshift. The pipeline enables easy analysis of user activity for a fictional music streaming service.

## Project Overview

This data pipeline:
1. Extracts song and user activity data from S3
2. Stages the raw data in Redshift
3. Transforms the data into a star schema optimized for analytical queries
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
│   ├── aws_setup_dag.py      # DAG for setting up AWS resources
│   └── final_project.py      # Main ETL pipeline DAG
├── plugins/
│   ├── helpers/              # Helper modules
│   │   └── sql_queries.py    # SQL transformation queries
│   └── operators/            # Custom Airflow operators
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

4. Access the Airflow web UI at http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

5. Configure connections in Airflow:
   - Navigate to Admin > Connections
   - Add an `aws_credentials` connection with your AWS access key, secret key, and session token
   - Add a `redshift` connection with your Redshift serverless endpoint details

6. Run the setup DAG first to create AWS resources (if needed):
   - Trigger the `aws_setup_dag`

7. Once setup is complete, trigger the main ETL pipeline:
   - Trigger the `final_project` DAG

## AWS Setup DAG

The `aws_setup_dag` automates the entire AWS infrastructure provisioning process. This DAG is designed to be run once before executing the main ETL pipeline.

### DAG Structure

The `aws_setup_dag` follows these sequential steps:

1. **Load Configuration** - Reads the AWS and Redshift configuration from the `config/iac.cfg` file
2. **Verify AWS Connection** - Validates that the AWS credentials are properly configured in Airflow
3. **Test AWS Connection** - Tests connectivity to AWS services
4. **Create IAM Role** - Creates an IAM role for Redshift with appropriate permissions to access S3
5. **Create Redshift Serverless** - Provisions a Redshift Serverless namespace and workgroup
6. **Check Redshift Status** - Waits for the Redshift resources to become available
7. **Setup Redshift Connection** - Configures the Airflow connection to Redshift
8. **Create Tables** - Creates the necessary staging and analytics tables in Redshift

### Key Features

- **Error Handling** - Comprehensive error handling with detailed logging
- **Idempotence** - Can be run multiple times safely (cleans up existing resources if needed)
- **Configuration Management** - Uses a centralized configuration file for all AWS resources
- **Session Token Support** - Properly handles AWS temporary credentials with session tokens

### Configuration Parameters

The `aws_setup_dag` uses the following configuration parameters from `config/iac.cfg`:

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

### Running the Setup DAG

To run the setup DAG:

1. Ensure your AWS credentials are configured in Airflow (Admin > Connections > aws_credentials)
2. Navigate to the DAGs view in the Airflow UI
3. Find `aws_setup_dag` and click the "Trigger DAG" button
4. Monitor the task progress in the Graph or Tree view
5. Once all tasks show as successful, your AWS infrastructure is ready for the ETL pipeline

### Important Notes

- The setup DAG requires appropriate AWS permissions to create IAM roles and Redshift resources
- The process takes approximately 5-10 minutes to complete
- Redshift Serverless incurs costs as long as it's running (~$0.5-1 per hour depending on configuration)
- Remember to clean up AWS resources when they're no longer needed to avoid unnecessary charges

## Data Sources

This project uses two datasets stored in S3:

1. **Song Data**: JSON files containing song metadata
   - Location: `s3://udacity-dend/song_data`
   - Example fields: song_id, title, artist_id, year, duration

2. **Log Data**: JSON files containing user activity logs
   - Location: `s3://udacity-dend/log_data`
   - Example fields: userId, firstName, lastName, gender, level, location, userAgent

## Customization

- Modify `sql_queries.py` to change the transformation logic
- Adjust the DAG schedule in `final_project.py` to change the execution frequency
- Update the data quality checks in the DAG definition to implement additional validation rules

## Troubleshooting

If you encounter any issues:

1. Check the Airflow task logs for detailed error messages
2. Verify your AWS credentials and permissions
3. Ensure your Redshift Serverless instance is running
4. Confirm that the S3 buckets are accessible

## Acknowledgments

- This project was developed as part of the Udacity Data Engineering Nanodegree
- The dataset is provided by [Million Song Dataset](http://millionsongdataset.com/) and user activity simulation