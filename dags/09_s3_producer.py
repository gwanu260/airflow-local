# 로컬에서 작업(추출, 변환, 적제)후 -> csv 생성 -> s3 내(버킷/income/xx.csv) 업로드
# 1. 모듈 가져오기
from datetime import datetime, timedelta 
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 2. 상수, 환경변수, 고정값
# 버킷명
BUCKET_NAME = 'airflow-ai-en-4'
# 업로드할 파일명
FILE_NAME   = 'trigger_data.csv'
# 최종 버킷내 특정 폴더에 생성될 파일명 -> KEY
S3_KEY      = f'income/{FILE_NAME}'
# 업로드할 파일의 로컬내 위치
LOCAL_PATH  = f'/opt/airflow/dags/data/{FILE_NAME}'

# 3. DAG 정의
with DAG(
    dag_id              = "09_s3_producer",
    description         = "s3의 특정 버킷에 데이터 공급",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = None, # 수동실행 (트리거)
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['aws', 's3', '']
) as dag:
    # 3-1. 오퍼레이터 정의
    # 파일 생성
    create_csv_file_task = BashOperator(
        task_id      = 'create_csv_file_task',
        bash_command = f'echo "id,timestamp,value\n1,$(date),100\n2$(date),200" > {LOCAL_PATH}'
    )
    # 로컬 파일 -> s3 업로드
    # LocalFilesystemToS3Operator : 설정만으로 업로드 가능함
    upload_to_s3_task = LocalFilesystemToS3Operator(
        task_id     = 'upload_to_s3_task',
        filename    = LOCAL_PATH,    # 원데이터가 저장되어 있는 위치+파일명
        dest_key    = S3_KEY,        # 특정 데이터의 파일명
        dest_bucket = BUCKET_NAME,   # 파일이 업로드될 최종지(목적지) 버킷명
        aws_conn_id = 'aws_default', # aws 연결 정보 id, 대시보드상 등록한  id
        replace     = True           # 동일한 파일이 존재하면 덮어써라
    )
    
    # 3-2. 의존성
    create_csv_file_task >> upload_to_s3_task