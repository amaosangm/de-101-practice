from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from download_data import download_backblaze_data  # 假设这是你的下载函数
from process_data import process_drive_data  # 假设这是你的处理函数

# 设置默认参数
default_args = {
    'owner': 'Linhao',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'retries': 1,
}

# 创建DAG
dag = DAG(
    'backblaze_data_dag',
    default_args=default_args,
    description='A DAG to download and process drive data',
    schedule_interval=None,
    catchup=False,
)

# 下载数据的任务
download_task = PythonOperator(
    task_id='download_data_task',
    python_callable=download_backblaze_data,
    dag=dag,
)

# 数据处理的任务
process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_drive_data,
    op_kwargs={
        'data_lake_path': '/Users/linhao.jia/Documents/TWCode/DE/de-101-practice/data_lake/',
        'daily_output_path': '/Users/linhao.jia/Documents/TWCode/DE/de-101-practice/output/daily',
        'yearly_output_path': '/Users/linhao.jia/Documents/TWCode/DE/de-101-practice/output/yearly',
    },
    dag=dag,
)

# 设置任务依赖关系
download_task >> process_task
