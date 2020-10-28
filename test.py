from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.utils.dates import days_ago

from airflow.models.dagrun import DagRun
from airflow.models.dagrun.DagRun import get_task_instance
from airflow.models.dag import DAG

from datetime import timedelta, datetime
import time
# from airflow.models import Variable

# ################################################ FUNCTION ################################################

def write_function():
    time.sleep(360)

# #### Airflow DAG COnfig ####
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


dag = DAG(
    'dag_test', max_active_runs=1, default_args=default_args, schedule_interval='*/4 * * * *', dagrun_timeout=timedelta(minutes=60))

# ##### USING ACTUAL SECRETS
# secrets = []
# env_vars = []


# #### Tasks #### 

# #1. Retrieve files
dag_test_one =  DummyOperator(task_id='run_this_first', dag=dag)

# #2. Cash
dag_test_two = PythonOperator(
                        provide_context=False,
                        task_id="WriteMongo1",
                        python_callable=write_function,
                        #requirements=["pymongo"],
                        dag=dag
                        )

dag_test_one >> dag_test_two