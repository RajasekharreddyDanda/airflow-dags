from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('process_s3_file_with_spark', default_args=default_args, schedule_interval=None)

# Sensor to detect the arrival of a new file in the S3 path
s3_key_sensor = S3KeySensor(
    task_id='s3_key_sensor',
    poke_interval=60,  # Check S3 every 60 seconds
    timeout=600,  # Wait for up to 600 seconds for the file to arrive
    aws_conn_id='aws_default',  # Connection ID to AWS
    bucket_name='food-delivery-analytics',  # S3 bucket name
    bucket_key='food_delivery_data.csv',  # Path to the file in S3
    dag=dag
)

# Spark job configuration
spark_steps = [{
    'Name': 'Spark processing step',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
            '--deploy-mode', 'cluster',
            '--master', 'yarn',
            's3://s3-emr-scripts/emr-redshit.py',  # Path to your Spark job script in S3
        ],
    },
}]

# Operator to add Spark job steps to EMR cluster
add_steps = EmrAddStepsOperator(
    task_id='add_spark_steps',
    job_flow_id='j-28IH021N6P0IH',  # EMR cluster ID
    aws_conn_id='aws_default',  # Connection ID to AWS
    steps=spark_steps,
    dag=dag
)

# Sensor to wait for Spark job completion
step_sensor = EmrStepSensor(
    task_id='watch_step',
    job_flow_id='j-28IH021N6P0IH',  # EMR cluster ID
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',  # Connection ID to AWS
    dag=dag
)

s3_key_sensor >> add_steps >> step_sensor
