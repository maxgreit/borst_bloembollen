from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Venv activerings functie
def venv_command(script_path):
    return f"source /home/greit/klanten/borst/borst_venv/bin/activate && python3 {script_path}"


# Definieer de standaardinstellingen voor de DAG
default_args = {
    'owner': 'Max - Greit',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['max@greit.nl'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definieer de DAG
dag = DAG(
    'borst_daily_dag_v01',
    default_args=default_args,
    description='Data update',
    schedule_interval="0 8 * * *",
    catchup=False,
)

# Definieer de taken van de DAG
fust_dagelijks_taak = BashOperator(
        task_id='fust_dagelijks',
        bash_command=venv_command("/home/greit/klanten/borst/bulbmanager/fust/fust_dagelijks.py"),
        dag=dag,
)

partij_maat_dagelijks_taak = BashOperator(
        task_id='partij_maat_dagelijks',
        bash_command=venv_command("/home/greit/klanten/borst/bulbmanager/partij_maat/partij_maat_dagelijks.py"),
        dag=dag,
)

werknemers_taak = BashOperator(
        task_id='werknemers',
        bash_command=venv_command("/home/greit/klanten/borst/dyflexis/werknemers/werknemers_main.py"),
        dag=dag,
)

geregistreerde_uren_dagelijks_taak = BashOperator(
        task_id='geregistreerde_uren_dagelijks',
        bash_command=venv_command("/home/greit/klanten/borst/dyflexis/geregistreerde_uren/uren_huidig_jaar.py"),
        dag=dag,
)

start_parallel_tasks = EmptyOperator(
        task_id='start_parallel_tasks',
        dag=dag,
    )

end_parallel_tasks = EmptyOperator(
        task_id='end_parallel_tasks',
        dag=dag,
    )

# Taak structuur
start_parallel_tasks >> [
    fust_dagelijks_taak,
    partij_maat_dagelijks_taak,
    werknemers_taak,
    geregistreerde_uren_dagelijks_taak
] >> end_parallel_tasks
                          