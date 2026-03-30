from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "bash_operator_demo",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Chạy một câu lệnh bash đơn giản
    task_1 = BashOperator(task_id="print_date", bash_command="date")

    # 2. Chạy một script hoặc lệnh phức tạp hơn
    task_2 = BashOperator(
        task_id="run_script", bash_command='echo "Hello Airflow" && sleep 5'
    )

    task_1 >> task_2
