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
#from udacity.common.final_project_sql_statements import SqlQueries
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.postgres_operator import PostgresOperator



default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(), # Start now
    'depends_on_past': False, # No dependancies on past runs
    'retries': 3, # Retry 3 times in case of task failure
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
    'catchup': False, # No backfill past DAG runs
    'email_on_retry': False, # No email on retry
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    stop_operator = DummyOperator(task_id='Stop_execution')

    # CREATE TABLES TASKS
    # create_staging_events_table = PostgresOperator(
    #     task_id="create_staging_events_table",
    #     postgres_conn_id="redshift_default",
    #     sql=final_project_sql_statements.SqlQueries.staging_events_table_create 
    # )

    # create_staging_songs_table = PostgresOperator(
    #     task_id="create_staging_songs_table",
    #     postgres_conn_id="redshift_default",
    #     sql=final_project_sql_statements.SqlQueries.staging_songs_table_create
    # )

    # create_songplay_table = PostgresOperator(
    #     task_id="create_songplay_table",
    #     postgres_conn_id="redshift_default",
    #     sql=final_project_sql_statements.SqlQueries.songplay_table_create
    # )

    # create_user_table = PostgresOperator(
    #     task_id="create_user_table",
    #     postgres_conn_id="redshift_default",
    #     sql=final_project_sql_statements.SqlQueries.user_table_create
    # )

    # create_song_table = PostgresOperator(
    #     task_id="create_song_table",
    #     postgres_conn_id="redshift_default",
    #     sql=final_project_sql_statements.SqlQueries.song_table_create
    # )

    # create_artist_table = PostgresOperator(
    #     task_id="create_artist_table",
    #     postgres_conn_id="redshift_default",
    #     sql=final_project_sql_statements.SqlQueries.artist_table_create
    # )

    # create_time_table = PostgresOperator(
    #     task_id="create_time_table",
    #     postgres_conn_id="redshift_default",
    #     sql=final_project_sql_statements.SqlQueries.time_table_create
    # )

    # LOAD DATA TASKS
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift_default",
        aws_credentials_id="aws_default",
        table="staging_events",
        s3_bucket="kgolovko-data-pipelines",
        s3_key="log-data",  # Dynamic path based on execution date
        json_path="log_json_path.json",  # the JSON format
        iam_role="arn:aws:iam::867587926746:role/my-redshift-service-role",
        region="us-east-1"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift_default",
        aws_credentials_id="aws_default",
        table="staging_songs",
        s3_bucket="kgolovko-data-pipelines",
        s3_key="song-data",  # Pattern to load all files from a day
        json_path="auto",  # Auto-detect the JSON format
        iam_role="arn:aws:iam::867587926746:role/my-redshift-service-role",
        region="us-east-1"
    )


    # Fact table load
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift_default",
        target_table="songplay",
        append_only=False,
        sql_query=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    # Dimension table loads
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

    # Data quality check
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift_default",
        #tables=["songplay", "user_info", "song", "artist", "time"],
        sql_queries = [
            # Data Quality Check 1: At least one row in the songplays table
            #"SELECT COUNT(*) FROM songplay WHERE songplay_id IS NOT NULL",  # Ensures at least one record in songplay table

            # Data Quality Check 2: No NULL user_ids in the user_info table
            "SELECT COUNT(*) FROM user_info WHERE userid IS NULL",  # Ensures there are no NULL user_ids

            # Data Quality Check 3: At least one unique song ID in the song table
            #"SELECT COUNT(DISTINCT song_id) FROM song WHERE song_id IS NOT NULL",  # Ensures there is at least one unique song_id

            # Data Quality Check 4: No NULL artist names in the artist table
            "SELECT COUNT(*) FROM artist WHERE artist_name IS NULL",  # Ensures there are no NULL artist names

            # Data Quality Check 5: No NULL start_time records in the time table
            "SELECT COUNT(*) FROM time WHERE start_time IS NULL"  # Ensures there are no NULL start_time values
        ],
        expected_results = [
            #1,  # At least one row in songplay
            0,  # No NULL user_ids in user_info
            #1,  # At least one unique song ID in the song table
            0,  # No NULL artist names in the artist table
            0   # No NULL start_time records in the time table
        ]
    )

    # Task dependancies
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    # start_operator >> [
    #     create_staging_events_table,
    #     create_staging_songs_table,
    #     create_songplay_table,
    #     create_user_table,
    #     create_song_table,
    #     create_artist_table,
    #     create_time_table
    # ]

    # [
    #     create_staging_events_table,
    #     create_staging_songs_table
    # ] >> stage_events_to_redshift

    # create_songplay_table >> load_songplays_table
    # create_user_table >> load_user_dimension_table
    # create_song_table >> load_song_dimension_table
    # create_artist_table >> load_artist_dimension_table
    # create_time_table >> load_time_dimension_table

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> stop_operator

final_project_dag = final_project()