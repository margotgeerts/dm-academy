from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator
from datetime import datetime

args = {
    'owner': 'airflow',
}

with DAG(
    'submitjob',
    default_args=args,
    description='DAG for triggering batch job',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 2, 11),
    catchup=False,
    tags=['example'],
) as dag:
    AwsBatchOperator(
        task_id='trigger-job',
        job_name='margot-job-1',
        job_definition='arn:aws:batch:eu-west-1:338791806049:job-definition/Margot-batch-job:1',
        job_queue='arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-winter-2022-job-queue',
        overrides = {}
    )