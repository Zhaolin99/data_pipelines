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
@dag(
    dag_id = 'dag-zh',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
    )
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
    )
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )
    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, \
    load_artist_dimension_table, load_time_dimension_table ] >> run_quality_checks
    run_quality_checks >> end_operator
final_project_dag = final_project()