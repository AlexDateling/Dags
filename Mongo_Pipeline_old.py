from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

Mongodag = DAG(
    'Mongo_read_write_old', default_args=default_args, schedule_interval=timedelta(minutes=10))

################################################ TASKS ################################################
start = DummyOperator(task_id='run_this_first', dag=Mongodag, executor_config={"KubernetesExecutor": {"image": "apache/airflow:1.10.10-python3.6"}})

#Create different images
    #each will have different docker images

Write = KubernetesPodOperator(namespace='default',
                          image="02496024/airwrite:v1.0",
                          name="write-test",
                          task_id="WriteMongo1",
                          get_logs=True,
                          env_vars={
                              'MONGO1_MONGODB_SERVICE_PORT':'{{ var.value.MONGO1_MONGODB_SERVICE_PORT }}',
                              'MONGODB1_USERNAME':'{{ var.value.MONGODB1_USERNAME }}',
                              'MONGODB1_DATABASE':'{{ var.value.MONGODB1_DATABASE }}',
                              'MONGODB1_ROOT_PASSWORD':'{{ var.value.MONGODB1_ROOT_PASSWORD }}',
                              'MONGODB1_PASSWORD':'{{ var.value.MONGODB1_PASSWORD }}',
                              'MONGO1_HOST_T':'{{ var.value.MONGO1_HOST_T }}'
                              },
                          dag=Mongodag
                          )

#Transfer Mongo 1 Data to Mongo 2 Database

Transfer = KubernetesPodOperator(namespace='default',
                          image="02496024/airtransfer:v1.0",
                          name="transfer-test",
                          task_id="TransferMongo2",
                          get_logs=True,
                          env_vars={# Database 1
                                    'MONGO1_MONGODB_SERVICE_PORT':'{{ var.value.MONGO1_MONGODB_SERVICE_PORT }}',
                                    'MONGODB1_USERNAME':'{{ var.value.MONGODB1_USERNAME }}',
                                    'MONGODB1_DATABASE':'{{ var.value.MONGODB1_DATABASE }}',
                                    'MONGODB1_ROOT_PASSWORD':'{{ var.value.MONGODB1_ROOT_PASSWORD }}',
                                    'MONGODB1_PASSWORD':'{{ var.value.MONGODB1_PASSWORD }}',
                                    'MONGO1_HOST_T':'{{ var.value.MONGO1_HOST_T }}',
                                    # Database 2
                                    'MONGO2_MONGODB_SERVICE_PORT':'{{ var.value.MONGO2_MONGODB_SERVICE_PORT }}',
                                    'MONGODB2_USERNAME':'{{ var.value.MONGODB2_USERNAME }}',
                                    'MONGODB2_DATABASE':'{{ var.value.MONGODB2_DATABASE }}',
                                    'MONGODB2_ROOT_PASSWORD':'{{ var.value.MONGODB2_ROOT_PASSWORD }}',
                                    'MONGODB2_PASSWORD':'{{ var.value.MONGODB2_PASSWORD }}',
                                    'MONGO2_HOST_T':'{{ var.value.MONGO2_HOST_T }}'
                                    },
                          dag=Mongodag
                          )

#Read From Mongo Database 2

Read = KubernetesPodOperator(namespace='default',
                          image="02496024/airread:v1.0",
                          name="read-test",
                          task_id="ReadMongo3",
                          get_logs=True,
                          env_vars={'MONGO2_MONGODB_SERVICE_PORT':'{{ var.value.MONGO2_MONGODB_SERVICE_PORT }}',
                                    'MONGODB2_USERNAME':'{{ var.value.MONGODB2_USERNAME }}',
                                    'MONGODB2_DATABASE':'{{ var.value.MONGODB2_DATABASE }}',
                                    'MONGODB2_ROOT_PASSWORD':'{{ var.value.MONGODB2_ROOT_PASSWORD }}',
                                    'MONGODB2_PASSWORD':'{{ var.value.MONGODB2_PASSWORD }}',
                                    'MONGO2_HOST_T':'{{ var.value.MONGO2_HOST_T }}',
                                    },
                          dag=Mongodag
                          )


################################################ ORDER ################################################
Write.set_upstream(start)
Transfer.set_upstream(Write)
Read.set_upstream(Transfer)
