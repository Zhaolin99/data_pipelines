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
from airflow import DAG 
default_args = {
    'owner': 'airflow-zh',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': False,

}

with DAG('dag-zh',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@daily') as dag:

    def final_project():
        start_operator = DummyOperator(task_id='Begin_execution')
        stage_events_to_redshift = StageToRedshiftOperator(
            task_id='Stage_events',
            redshift_conn_id='redshift',
            aws_credentials_id='aws_credentials',
            table='staging_events',
            s3_bucket='s3://airflow-1-zh/log_data',
            json_path='s3://airflow-1-zh/log_json_path.json'
        )
        stage_songs_to_redshift = StageToRedshiftOperator(
            task_id='Stage_songs',
            redshift_conn_id='redshift',
            aws_credentials_id='aws_credentials',
            table='staging_songs',
            s3_bucket='s3://airflow-1-zh/song_data',
            json_path='s3://airflow-1-zh/song_json_path.json'
        )


        load_songplays_table = LoadFactOperator(
            task_id='Load_songplays_fact_table',
            redshift_conn_id="redshift",
            table="songplays",
            sql_query=final_project_sql_statements.songplay_table_insert,
            append_data=True
        )


        load_user_dimension_table = LoadDimensionOperator(
            task_id='Load_user_dim_table',
            redshift_conn_id="redshift",
            table="users",
            sql_query=final_project_sql_statements.user_table_insert,
            append_data=False

        )
        load_song_dimension_table = LoadDimensionOperator(
            task_id='Load_song_dim_table',
            redshift_conn_id="redshift",
            table="songs",
            sql_query=final_project_sql_statements.song_table_insert,
            append_data=False

        )
        load_artist_dimension_table = LoadDimensionOperator(
            task_id='Load_artist_dim_table',
            redshift_conn_id="redshift",
            table="artists",
            sql_query=final_project_sql_statements.artist_table_insert,
            append_data=False

        )
        load_time_dimension_table = LoadDimensionOperator(
            task_id='Load_time_dim_table',
            redshift_conn_id="redshift",
            table="time",
            sql_query=final_project_sql_statements.time_table_insert,
            append_data=False
        )

        run_quality_checks = DataQualityOperator(
            task_id='Run_data_quality_checks',
            redshift_conn_id="redshift",
            tables=["users", "songs",  "artists", "time"]
        )

        end_operator = DummyOperator(task_id='Stop_execution')

        start_operator >> stage_events_to_redshift
        start_operator >> stage_songs_to_redshift
        stage_events_to_redshift >> load_songplays_table
        stage_songs_to_redshift >> load_songplays_table
        load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, \
        load_artist_dimension_table, load_time_dimension_table ] >> run_quality_checks
        run_quality_checks >> end_operator
    final_project_dag = final_project()