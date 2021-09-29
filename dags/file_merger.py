
import sys
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import Variable
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
import scripts.archive_files as af

# Define some default settings
default_args = {
    'owner': 'ojb',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Declare some paths
this_dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(r'%s' %(this_dir_path + "/../"))

input_source_1 = parent_dir + "/input_source_1/"
input_source_2 =  parent_dir + "/input_source_2/"

sys.path.insert(0, parent_dir + "/scripts")
input_source_archive = parent_dir + "/input_source_archive/"

def failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
        print("File sensor timed out! Check the input source directory for more info.")

with DAG('file_merger',
         start_date=datetime(2021, 9, 27, 4, 0, 0),
         max_active_runs=3,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    pyspark_app_home=Variable.get("PYSPARK_APP_HOME")

    # Check for the first file every 30 seconds. Timeout after 1 hour
    wait_for_file_1 = FileSensor(
        task_id='wait_for_file_1',
        poke_interval=30,
        timeout=60 * 60,
        mode='reschedule',
        filepath=f'''{input_source_1}/*.json''',
        on_failure_callback = failure_callback
    )

    wait_for_file_2 = FileSensor(
        task_id='wait_for_file_2',
        poke_interval=30,
        timeout=60 * 60,
        mode='reschedule',
        filepath=f'''{input_source_2}/*.csv''',
        on_failure_callback = failure_callback
    )

    spark_app = SparkSubmitOperator(
        task_id="file_merger_spark",
        application=f'{pyspark_app_home}/file_merger_spark.py',
        conn_id="spark_local",
        verbose=0
    )

    archive_input_source_1_files = PythonOperator(
        task_id="archive_input_source_1_files",
        python_callable=af.archive_files,
        op_kwargs={"input_dir": input_source_1, 
                   "archive_dir": f'''{input_source_archive}/input_source_1/'''
                   }
    )

    archive_input_source_2_files = PythonOperator(
        task_id="archive_input_source_2_files",
        python_callable=af.archive_files,
        op_kwargs={"input_dir": input_source_2, 
                   "archive_dir": f'''{input_source_archive}/input_source_2/'''}
    )

    chain([wait_for_file_1, wait_for_file_2], spark_app, [archive_input_source_1_files, archive_input_source_2_files])