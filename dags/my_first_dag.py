from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Paramètres par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
dag = DAG(
    'my_first_dag',
    default_args=default_args,
    description='Mon premier DAG Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Tâche d'extraction de données
def extract(**kwargs):
    data = {"name": "Alice", "age": 25}
    print("Données extraites :", data)
    return data

# Tâche de transformation de données
def transform(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')
    transformed_data = {"name": extracted_data["name"].upper(), "age": extracted_data["age"] + 1}
    print("Données transformées :", transformed_data)
    return transformed_data

# Tâche de chargement des données
def load(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')
    print("Données chargées :", transformed_data)

def print_params(**kwargs):
    dag_run = kwargs.get('dag_run')
    params = dag_run.conf if dag_run else {}
    user = params.get('user', 'no user provided')
    print(f"Username received: {user}")

def launch_hello(**kwargs):
    dag_run = kwargs.get('dag_run')
    user = None
    if dag_run and dag_run.conf:
        user = dag_run.conf.get('user')
    print("Hello Airflow,", user)

def update_variable(**kwargs):
    current_user = Variable.get("user", default_var="default_user")
    new_user = current_user + "_updated"
    Variable.set("user", new_user)
    ti = kwargs['ti']
    ti.xcom_push(key='updated_user', value=new_user)

def say_hello(**kwargs):
    ti = kwargs['ti']
    user = ti.xcom_pull(task_ids='update_variable', key='updated_user')
    print(f"Hello Airflow, {user}!")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=say_hello,
    provide_context=True,
    dag=dag,
)

# Définir les opérateurs
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

update_variable_task = PythonOperator(
    task_id='update_variable',
    python_callable=update_variable,
    provide_context=True,
    dag=dag,
)

# Définir les dépendances
update_variable_task >> hello_task >> extract_task >> transform_task >> load_task
