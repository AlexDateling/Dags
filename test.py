from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.utils.dates import days_ago

from airflow.models.dagrun import DagRun
from airflow.models.dag import DAG

from datetime import timedelta, datetime

# from airflow.models import Variable

# ################################################ FUNCTION ################################################

def write_function():
    print("Test Dag task 2")
    print("obtaining dag details")
    dag_run = DagRun()
    dag= dag_run.get_dag()
    print(dag.execution_date)
    print(dag_run.start_date)
    task_instance = dag_run.get_task_instance(task_id='run_this_first')
    print(task_instance.start_date)
    
    # print(DagRun.start_date)
    # print(DagRun.execution_date)

    # dag = DagRun.get_dag()
    # print(dag.get_run_dates())

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
    'dag_test', default_args=default_args, schedule_interval='30 15 * * *', dagrun_timeout=timedelta(minutes=60))

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