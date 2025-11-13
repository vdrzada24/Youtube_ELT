from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import get_playlist_id,get_video_id,extract_video_data,save_to_json
from datawarehouse.dwh import staging_table,core_table
from data_quality.soda import yt_elt_dq
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_timzone = pendulum.timezone("Europe/Malta")

default_args = {
    "owner" : "vraizada",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "raizadavedant24@gmail.com",
    #"retries" : 1,
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date" : datetime(2025,1,1,tzinfo=local_timzone)


}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id = "producer_json",
    default_args = default_args,
    description = "DAG to produce json file using raw data",
    schedule = '0 14 * * *' ,
    catchup = False
) as dag_produce :
    
    #tasks
    playlist_Id = get_playlist_id()
    video_ids = get_video_id(playlistId = playlist_Id)
    extract_data = extract_video_data(video_ids=video_ids)
    save_to_json_task = save_to_json(extracted_data=extract_data)

    trigger_update_database = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_database",
    )

    # dependencies..
    playlist_Id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_database

with DAG(
    dag_id = "update_database",
    default_args = default_args,
    description = "DAG to parse JSON file and apply tansform and load data to staging and core schema",
    schedule = None ,
    catchup = False
) as dag_update :
    
    #tasks
    staging = staging_table()
    core = core_table()

    trigger_data_quality_checks = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality_checks",
    )

    # dependencies..
    staging >> core >> trigger_data_quality_checks

with DAG(
    dag_id = "data_quality_checks",
    default_args = default_args,
    description = "DAG to check the data quality for both schemas",
    schedule = None ,
    catchup = False
) as dag :
    
    #tasks
    soda_staging = yt_elt_dq(staging_schema)
    soda_core = yt_elt_dq(core_schema)

    # dependencies..
    soda_staging >> soda_core

