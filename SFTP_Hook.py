from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow.contrib.sensors.sftp_sensor import SFTPSensor

from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.utils.dates import days_ago

def transferfile():
    sftpstore = SFTPHook(ftp_conn_id='sftp', port=22)
    sftpstore.get_conn()
    sftpstore.retrieve_file("/test/cUsers.js","/tmp/test/users.txt")
    sftpstore.close_conn()

################################################ DAG CONFIG ################################################

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['ADateling2@Gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

SFTPDAG = DAG(
    'SFTP_DAG', default_args=default_args, schedule_interval='0 0 * * *', dagrun_timeout=timedelta(minutes=60))

################################################ TASKS ################################################

start = DummyOperator(task_id='run_this_first',
                    dag=SFTPDAG
                    )

sftpsensor = SFTPSensor(task_id='sensor_work', sftp_conn_id='sftp', path="/test/cUsers.js", dag=SFTPDAG)

start >> sftpsensor

sftpsensorfail = SFTPSensor(task_id='sensor_fail', sftp_conn_id='sftp', path="/test/fail.txt", dag=SFTPDAG)

start >> sftpsensorfail

readtransfer = PythonOperator(task_id="transfer_file",
                    provide_context=False,
                    python_callable=transferfile,
                    executor_config={
                        "KubernetesExecutor": {
                            "volumes": [
                                {
                                "name": "my-volume",
                                "persistentVolumeClaim":
                                    {
                                    "claimName": "pvc-shared"
                                    }
                                }
                            ],
                            "volume_mounts": [
                                {
                                "name": "my-volume",
                                "mountPath": "/tmp/test"
                                }      
                            ]
                        }
                    },
                    dag=SFTPDAG
                    )

sftpsensor >> readtransfer