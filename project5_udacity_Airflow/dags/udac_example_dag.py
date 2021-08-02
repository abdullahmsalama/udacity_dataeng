from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator,\
LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          #schedule_interval = '0 * * * *'
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id = 'begin_execution',  dag = dag)

create_staging_events_table = PostgresOperator(
    task_id = 'create_staging_events_table',
    postgres_conn_id = 'redshift',
    sql = SqlQueries.staging_events_table_create,
    dag = dag
)

create_staging_songs_table = PostgresOperator(
    task_id = 'create_staging_songs_table',
    postgres_conn_id = 'redshift',
    sql = SqlQueries.staging_songs_table_create,
    dag = dag
)

create_artists_table = PostgresOperator(
    task_id = 'create_artists_table',
    postgres_conn_id = 'redshift',
    sql = SqlQueries.artists_table_create,
    dag = dag
)

create_users_table = PostgresOperator(
    task_id = 'create_users_table',
    postgres_conn_id = 'redshift',
    sql = SqlQueries.users_table_create,
    dag = dag
)

create_songs_table = PostgresOperator(
    task_id = 'create_songs_table',
    postgres_conn_id = 'redshift',
    sql = SqlQueries.songs_table_create,
    dag = dag
)

create_time_table = PostgresOperator(
    task_id = 'create_time_table',
    postgres_conn_id = 'redshift',
    sql = SqlQueries.time_table_create,
    dag = dag
)

create_songplays_table = PostgresOperator(
    task_id = 'create_songplays_table',
    postgres_conn_id = 'redshift',
    sql = SqlQueries.songplays_table_create,
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events',
    table_name = 'staging_events',
    s3_bucket_name = 'udacity-dend',
    s3_data_path = 'log_data',
    copy_option = "JSON 's3://udacity-dend/log_json_path.json'",
    dag = dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    table_name = 'staging_songs',
    s3_bucket_name = 'udacity-dend',
    s3_data_path = 'song_data',
    copy_option = "FORMAT AS JSON 'auto'",
    dag = dag
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id = 'load_artists_dim_table',
    table_name = 'artists',
    insert_sql = SqlQueries.artists_table_insert,
    truncate_table = 1,
    redshift_conn_id = 'redshift',
    dag = dag
)

load_users_dimension_table = LoadDimensionOperator(
    task_id = 'load_users_dim_table',
    table_name = 'users',
    insert_sql = SqlQueries.users_table_insert,
    truncate_table = 1,
    redshift_conn_id = 'redshift',
    dag = dag
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id = 'load_songs_dim_table',
    table_name = 'songs',
    insert_sql = SqlQueries.songs_table_insert,
    truncate_table = 1,
    redshift_conn_id = 'redshift',
    dag = dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'load_time_dim_table',
    table_name = 'time',
    insert_sql = SqlQueries.time_table_insert,
    truncate_table = 1,
    redshift_conn_id = 'redshift',
    dag = dag
)

load_songplays_table = LoadFactOperator(
    task_id = 'load_songplays_fact_table',
    table_name = 'songplays',
    insert_sql = SqlQueries.songplays_table_insert,
    truncate_table = 1,
    redshift_conn_id = 'redshift',
    dag = dag
)


run_quality_checks = DataQualityOperator(
    task_id = 'run_data_quality_checks',
    redshift_conn_id = 'redshift',
    dag = dag,
    quality_checks = [
        {
            'sql': 'select count(*) from users;',
            'expected_result': 0
        },
        {
            'sql': 'select count(*) from songs where songid is NULL;',
            'expected_result': 0
        }
    ]
)

end_operator = DummyOperator(task_id = 'stop_execution', dag = dag)


# DAGs' dependencies

# Start by creating all tables, first for staging then the fact and dimension tables.
start_operator >> [create_staging_events_table,\
                   create_staging_songs_table]

start_operator >> [create_artists_table,\
                   create_users_table,\
                   create_songs_table,\
                   create_time_table,\
                   create_songplays_table]
                   
    
# After creation, comes the staging tasks
create_staging_events_table >> [stage_events_to_redshift,\
                             stage_songs_to_redshift]

create_staging_songs_table >> [stage_events_to_redshift,\
                             stage_songs_to_redshift]

[create_artists_table, create_users_table,\
 create_songs_table, create_time_table,\
 create_songplays_table] >> stage_events_to_redshift
                             
[create_artists_table, create_users_table,\
 create_songs_table, create_time_table,\
 create_songplays_table] >> stage_songs_to_redshift

# And the staging is just before loading the data to their tables
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_artists_dimension_table,\
                         load_users_dimension_table,\
                         load_songs_dimension_table,\
                         load_time_dimension_table]
                         
# Before finishing we have to run the quality checks
[load_artists_dimension_table,\
 load_users_dimension_table,\
 load_songs_dimension_table,\
 load_time_dimension_table] >> run_quality_checks

# Finally go the end operator that signals end of execution
run_quality_checks >> end_operator
