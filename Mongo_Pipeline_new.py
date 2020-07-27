from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.mongo_hook import MongoHook

import pymongo
import os

################################################ FUNCTION ################################################

def write_function():
    try:
        mongo = MongoHook(conn_id="Mongo1")
        myclient = mongo.get_conn()
        db = "airflowmongo"
        mydb = myclient[db]
        mycol = mydb["customers"]

        mydict = { "name": "John", "Surname": "Highway 37", "Username":"chicken" }

        mycol.insert_one(mydict)
        print("inserted into dataase")
    except:
        print("can't login")


################################################ DAG CONFIG ################################################


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['ADateling2@Gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

Mongodag = DAG(
    'Mongo_read_write_new', default_args=default_args, schedule_interval='0 0 * * *', dagrun_timeout=timedelta(minutes=60))


################################################ TASKS ################################################

start = DummyOperator(task_id='run_this_first',
                    dag=Mongodag
                    )
#, executor_config={"KubernetesExecutor": {"image": "airflow:latest"}}
## Sensor to check whether the mongodb is online
CheckNode = HttpSensor(
                        http_conn_id='nodeapi',
                        method='GET',
                        endpoint="",
                        task_id="checknode",
                        response_check=lambda response: True if "May Proceed" in response.text else False,
                        poke_interval=5,    # this means it should send an http request every 5 seconds
                        timeout=20,	        # this means that it will wait 20seconds until timing out
                        dag=Mongodag
                        )
#Create different images
    #each will have different docker images

Write = PythonOperator(
                        provide_context=False,
                        task_id="WriteMongo1",
                        python_callable=write_function,
                        #requirements=["pymongo"],
                        dag=Mongodag
                        )

# queue a message to rabbitmq
# QueueMsg = 

#Transfer Mongo 1 Data to Mongo 2 Database

Transfer = KubernetesPodOperator(namespace='default',
                          image="02496024/airtransfer:v1.0",
                          name="transfer-test",
                          get_logs=True,
                          task_id="TransferMongo2",
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
CheckNode.set_upstream(start)
Write.set_upstream(CheckNode)
Transfer.set_upstream(Write)
Read.set_upstream(Transfer)