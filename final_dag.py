from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.operators.postgres_operator import PostgresOperator

"""
    Purpose of the script:
        - Define the DAG for orchestrating ETL processes in Redshift.
        - The script loads data from S3 into staging tables, then from staging tables into fact and dimension tables, followed by data quality checks.

    Inputs:
        - StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator and DataQualityOperator - import from operator scripts
        - Redshift connection (named `redshift_default` in Airflow)
        - AWS credentials (named `aws_default` in Airflow)
        - SQL queries and table names defined in `final_project_sql_statements`
        - S3 bucket and keys for log-data and song-data

    Outputs:
        - The following tasks within the DAG function in order of execution:
            1. Stage data from S3 into Redshift staging tables.
            2. Load data from Redshift staging tables into fact tables (songplay).
            3. Load data from Redshift staging tables into dimension tables (user_info, song, artist, time).
            4. Run data quality checks to ensure data integrity.
        
    Functionality:
        - Staging tasks: Load raw log and song data from S3 into staging tables in Redshift.
        - Fact table loading tasks: Insert processed data into the songplay fact table from Redshift staging tables.
        - Dimension table loading tasks: Insert transformed data into user, song, artist, and time dimension tables.
        - Data quality check tasks: Validate that there are no missing or invalid values in the key tables.
        - Default args:
            - Start now
            - No dependancies on past runs
            - Retry 3 times in case of task failure
            - Retries happen every 5 minutes
            - No backfill past DAG runs
            - No email on retry

    Task Dependencies:
        - Staging tables tasks run first.
        - Fact and dimension tables are loaded once the staging data have finished loading.
        - Data quality checks are executed after all loading tasks are completed.
        - The DAG runs starting from the `start_operator`, followed by staging, loading, and checking tasks, and ending at the `stop_operator`.
"""

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(), 
    'depends_on_past': False, 
    'retries': 3, 
    'retry_delay': timedelta(minutes=5), 
    'catchup': False, 
    'email_on_retry': False, 
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    """
    Purpose of the function:
        - This function defines the DAG for orchestrating the ETL workflow for the final project.
        - The DAG consists of tasks to load data into Redshift staging tables, fact tables, and dimension tables, followed by data quality checks.
    Inputs:
        - None
    Outputs:
        - The function returns the `final_project_dag` DAG, which can be seen in the Airflow UI.
    Functionality:
        - Load tables tasks: 
            - Stage log-data and song-data from S3 into Redshift using `StageToRedshiftOperator`.
            - Load the `songplay` fact table and the four dimension tables (`user_info`, `song`, `artist`, `time`) using `LoadFactOperator` and `LoadDimensionOperator`.
        - Data quality checks task:
            - After data is loaded, the `DataQualityOperator` ensures that there are no NULL values in the id columns in fact and dimension tables.
    """

    start_operator = DummyOperator(task_id='Begin_execution')
    stop_operator = DummyOperator(task_id='Stop_execution')

    # LOAD STAGING TABLES
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift_default",
        aws_credentials_id="aws_default",
        table="staging_events",
        s3_bucket="kgolovko-data-pipelines",
        s3_key="log-data",
        json_path="log_json_path.json", 
        iam_role="arn:aws:iam::8xxxx6:role/my-redshift-service-role",
        region="us-east-1"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift_default",
        aws_credentials_id="aws_default",
        table="staging_songs",
        s3_bucket="kgolovko-data-pipelines",
        s3_key="song-data",  
        json_path="auto",  
        iam_role="arn:aws:iam::8xxxx6:role/my-redshift-service-role",
        region="us-east-1"
    )


    # LOAD FACT TABLES
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift_default",
        target_table="songplay",
        append_only=False,
        sql_query=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    # LOAD DIMENSION TABLES
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.user_table_insert,
        redshift_conn_id="redshift_default",
        target_table="user_info",
        truncate=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.song_table_insert,
        redshift_conn_id="redshift_default",
        target_table="song"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.artist_table_insert,
        redshift_conn_id="redshift_default",
        target_table="artist"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.time_table_insert,
        redshift_conn_id="redshift_default",
        target_table="time"
    )

    # DATA QUALITY CHECKS
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift_default",
        sql_queries = [
            "SELECT COUNT(*) FROM songplay WHERE songplay_id IS NULL",
            "SELECT COUNT(*) FROM user_info WHERE userid IS NULL",
            "SELECT COUNT(*) FROM song WHERE song_id IS NULL",  
            "SELECT COUNT(*) FROM artist WHERE artist_id IS NULL",  
            "SELECT COUNT(*) FROM time WHERE start_time IS NULL"
        ],
        expected_results = [
            0, 
            0, 
            0,
            0,
            0
        ]
    )

    # TASK DEPENDANCIES
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> stop_operator

final_project_dag = final_project()