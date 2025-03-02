from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key='log_data',
        json_path='s3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        target_table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        target_table='users',
        sql_query=SqlQueries.user_table_insert,
        truncate_table=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        target_table='songs',
        sql_query=SqlQueries.song_table_insert,
        truncate_table=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        target_table='artists',
        sql_query=SqlQueries.artist_table_insert,
        truncate_table=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        target_table='time',
        sql_query=SqlQueries.time_table_insert,
        truncate_table=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        test_cases=[
            {'sql': 'SELECT COUNT(*) FROM songplays', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM users', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM songs', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM artists', 'expected_result': 0, 'operator': '>'},
            {'sql': 'SELECT COUNT(*) FROM time', 'expected_result': 0, 'operator': '>'}
        ]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    # Set dependencies
    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()