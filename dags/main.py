from dataquality.soda import yt_elt_data_quality
from datawarehouse.dwh import core_table
from datawarehouse.dwh import staging_table
from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from api.video_stats import get_playlist_id,get_video_ids,extract_video_data,save_to_json

local_tz = pendulum.timezone("Asia/Kolkata")
staging_schema = "staging"
core_schema = "core"

default_args = {
    "owner" : "dataengineers",
    "depends_on_past": False,
    "email_on_failure":False,
    "email_on_retry":False,
    # "retries":1,
    # "retry_delay":timedelta(minutes=5),
    "start_date":datetime(2026,1,1,tzinfo=local_tz),
    "max_active_runs":1,
    "dagrun_timeout":timedelta(hours=1)
}

with DAG(
    dag_id = "produce_json",
    default_args=default_args,
    description="Dag to produce json file with raw data",
    schedule='0 14 * * *',
    catchup=False,
) as dag:

    playlist_id_task = get_playlist_id()
    video_ids_task = get_video_ids(playlist_id_task)
    extracted_data_task = extract_video_data(video_ids_task)
    save_to_json_task = save_to_json(extracted_data_task)

    playlist_id_task >> video_ids_task >> extracted_data_task >> save_to_json_task

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # trigger_data_quality = TriggerDagRunOperator(
    #     task_id="trigger_data_quality",
    #     trigger_dag_id="data_quality",
    # )

    # Define dependencies
    update_staging >> update_core 
    # >> trigger_data_quality

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core