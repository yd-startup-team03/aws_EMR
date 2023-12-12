import datetime
import logging
import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import XCom
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessApplicationSensor, EmrServerlessJobSensor
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
    EmrServerlessStopApplicationOperator,
)
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from io import StringIO

day_time = 86400000
hook = S3Hook(aws_conn_id='aws_s3')
bucket ='taehun-s3-bucket-230717'
# Replace these with your correct values
JOB_ROLE_ARN =  "arn:aws:iam::796630047050:role/emr_serverless_exe_role"
S3_LOGS_BUCKET = bucket
DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": "s3://taehun-s3-bucket-230717/spark_logs/"}
    },
}


def list_keys(**context):
    hook = S3Hook(aws_conn_id='aws_s3')
    paginator = hook.get_conn().get_paginator('list_objects_v2')
    bucket ='taehun-s3-bucket-230717'
    current_time = datetime.now().strftime('%m월 %d일 %A %H시 %M분')
    current_folder = Variable.get('current_date_folder')
    prefix = f'{current_folder}/'
  
    file_size = 0
    # logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    # 파일 용량 체크
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
      for obj in page.get('Contents', []):
        file_size += obj['Size']
    # KB로 변환
    file_size = round(file_size / 1024, 2)
        
    len_keys = len(keys)
    # logging.info(f"len_key chk {len_keys}, {keys}")
    context['task_instance'].xcom_push(key='file_size',value=file_size)
    context['task_instance'].xcom_push(key='current_time',value=current_time)
    context['task_instance'].xcom_push(key='key_val', value=len_keys)
    context['task_instance'].xcom_push(key='prefix_val', value=prefix) 
    # new_current_folder = int(current_folder) + day_time
    # Variable.set('current_date_folder', new_current_folder)
    if len_keys == 0:
      return 'send_slack_fail'
    else:
      return 'send_slack_success'
    
def check_data(**context):
    hook = S3Hook(aws_conn_id='aws_s3')
    # paginator = hook.get_conn().get_paginator('list_objects_v2')
    bucket ='taehun-s3-bucket-230717'
    # current_time = datetime.now().strftime('%m월 %d일 %A %H시 %M분')
    current_folder = Variable.get('current_date_folder')
    prefix = f'tempdir/{current_folder}/checkData'

  
    # logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    
    check_message = ""
    for key in keys:
      if key.endswith('.json'):
        body = hook.read_key(key=key, bucket_name=bucket)
        data = StringIO(body)
        df = pd.read_json(data,lines=True)
        check_message += f"\n{key}:\n{df.head().to_string()}\n"

        # check_message += f"\n{df.head().to_string()}\n"
    context['task_instance'].xcom_push(key='check', value=check_message)
    # new_current_folder = int(current_folder) + day_time
    # Variable.set('current_date_folder', new_current_folder)
        
    





with DAG( 
    dag_id="medistream_logdata",
    schedule_interval=None,
    start_date=datetime(2023, 12, 6),
    tags=["Medistream"],
    catchup=False,
) as dag:
    token = Variable.get("slack_token")
    check_task = BranchPythonOperator(
      task_id='list_keys',
      python_callable=list_keys,
    )
    
    send_slack_fail = SlackAPIPostOperator(
      task_id='send_slack_fail',
      token = token,
      channel = '#일반',
      text = '적재된 로그 파일이 없습니다 (실패).',
    )
    
    send_slack_success = SlackAPIPostOperator(
      task_id='send_slack_success',
      token = token,
      channel = '#일반',
      text = """
        {{ task_instance.xcom_pull(task_ids='list_keys', key='current_time') }}
        Prefix: {{ task_instance.xcom_pull(task_ids='list_keys', key='prefix_val') }}
        file_size(total): {{ task_instance.xcom_pull(task_ids='list_keys', key='file_size') }} KB
        {{ task_instance.xcom_pull(task_ids='list_keys', key='key_val') }}개의 파일이 적재되었습니다.
    """,
    )
    # create_app = EmrServerlessCreateApplicationOperator(
    #     task_id="create_spark_app",
    #     job_type="SPARK",
    #     release_label="emr-6.15.0",
    #     config={"name": "airflow-test"},
    #     aws_conn_id='aws_s3',  # AWS 연결(ID) 지정
    # )
    application_id = "00ffc1vgng0gms2p"
    job1 = EmrServerlessStartJobOperator(
        task_id="start_job_1",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{bucket}/scripts/medi_project.jar",
                "entryPointArguments" :["--path","{{ task_instance.xcom_pull(task_ids='list_keys', key='prefix_val') }}","--token",token],
                "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g\
            --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
              
            
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
        aws_conn_id='aws_s3',  # AWS 연결(ID) 지정
    )
    
    # delete_app = EmrServerlessDeleteApplicationOperator(
    #     task_id="delete_app",
    #     application_id=application_id,
    #     trigger_rule="all_done",
    #     aws_conn_id='aws_s3',  # AWS 연결(ID) 지정
    # )
    stop_app = EmrServerlessStopApplicationOperator(
      task_id="stop_app",
      application_id = application_id,
      trigger_rule="all_done",
      force_stop=True,
      aws_conn_id='aws_s3'
    )
    spark_slack = SlackAPIPostOperator(
        task_id='spark_success',
        token = token,
        channel = '#일반',
        text ="spark 작업이 완료되었습니다."
    )
    check_data_task = PythonOperator(
        task_id = 'check_data',
        python_callable=check_data
    )
    test_data_slack = SlackAPIPostOperator(
        task_id='check_data_slack',
        token = token,
        channel = '#일반',
        text ="{{ task_instance.xcom_pull(task_ids='check_data', key='check') }}\n airflow 작업 완료"
    )
    spark_slack_fail_emr = SlackAPIPostOperator(
        task_id='spark_fail_emr',
        token = token,
        channel = '#일반',
        text ="Spark Job 이 실패하였습니다. Log 를 확인하세요 ",
        trigger_rule=TriggerRule.ALL_FAILED
    )
    
    check_task >> [send_slack_fail, send_slack_success]
    send_slack_success >> job1
    job1 >> stop_app >> spark_slack
    spark_slack >> check_data_task >> test_data_slack
    job1 >> spark_slack_fail_emr
