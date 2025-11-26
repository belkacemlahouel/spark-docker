from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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

# Définir les dépendances
extract_task >> transform_task >> load_task
