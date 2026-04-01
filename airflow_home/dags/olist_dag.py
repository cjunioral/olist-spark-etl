from airflow import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'cjunior',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olist_spark_medallion',
    default_args=default_args,
    description='Pipeline de Dados Olist - Bronze, Silver e Gold',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spark', 'olist', 'etl'],
) as dag:


    task_bronze = BashOperator(
        task_id='run_bronze',
        bash_command='python3 /home/cjunior/olist-spark-etl/src/bronze.py',
    )

    task_silver = BashOperator(
        task_id='run_silver',
        bash_command='cd /home/cjunior/olist-spark-etl && /home/cjunior/olist-spark-etl/venv/bin/python3 src/silver.py',
    )

    task_gold = BashOperator(
        task_id='run_gold',
        bash_command='cd /home/cjunior/olist-spark-etl && /home/cjunior/olist-spark-etl/venv/bin/python3 src/gold.py',
    )

    task_bronze >> task_silver >> task_gold