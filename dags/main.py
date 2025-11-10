from airflow import DAG
import pendulum 
from datetime import datetime, timedelta
from api.vedio_stats import get_playlist_id, get_vedio_ids, extract_vedio_data, save_to_json

local_tz = pendulum.timezone("America/New_York")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email':"data@engineers.com",
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(minutes=60),
    'start_date': datetime(2021, 1, 1, tzinfo=local_tz),
    #'end_date': None,
    
}

with DAG(
    dag_id="produce_json",
    description="DAG to produce JSON file with raw data",
    schedule_interval="0 14 * * *",   # runs daily at 14:00 local tz
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,

) as dag:
    
    #Define the tasks
    playlist_id = get_playlist_id()
    vedio_ids = get_vedio_ids(playlist_id)
    extract_data = extract_vedio_data(vedio_ids)
    save_to_json_task = save_to_json(extract_data)
    
    #Define the task dependencies
    playlist_id >> vedio_ids >> extract_data >> save_to_json_task
    

'''
@dag(
    dag_id="produce_json",
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",   # runs daily at 14:00 local tz
    start_date=datetime(2021, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args=default_args,
)
def produce_json():
    pid = get_playlist_id()
    vids = get_vedio_ids(pid)
    data = extract_vedio_data(vids)
    save_to_json(data)

produce_json()
'''