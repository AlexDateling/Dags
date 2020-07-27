from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.pod import Resources
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.utils.dates import days_ago

from airflow.models import dag

from datetime import timedelta, datetime

# from airflow.models import Variable

# ################################################ FUNCTION ################################################

def write_function():
    print("Test Dag task 2")
    print("obtaining dag details")
    print(dag.get_run_dates())

# #### Airflow DAG COnfig ####
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['ADateling2@Gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'dag_test', default_args=default_args, schedule_interval='30 15 * * *', dagrun_timeout=timedelta(minutes=60))

# ##### USING ACTUAL SECRETS
# secrets = []
# env_vars = []
volume_mount = VolumeMount('my-volume',
                            mount_path='/temp/',
                            sub_path=None,
                            read_only=False)

volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-shared'
      }
    }

volume = Volume(name='my-volume', configs=volume_config)

# #### Tasks #### 

# #1. Retrieve files
dag_test_one =  DummyOperator(task_id='run_this_first', dag=dag)

# #2. Cash
dag_test_two = PythonOperator(
                        provide_context=True,
                        task_id="WriteMongo1",
                        python_callable=write_function,
                        dag=dag
                        )

dag_test_one >> dag_test_two