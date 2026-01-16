# 로컬에서 파일 생성 => s3 업로드 => 업로드 여부 확인
# 1. 모듈 가져오기
from datetime import datetime, timedelta 
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 2. 상수, 환경변수등 고정값
# 본인이 사용하는 버킷명
BUCKET_NAME = 'airflow-ai-en-4'
# 업로드할 파일명
FILE_NAME   = 'hello.txt'
# 업로드할 파일의 로컬내 위치
LOCAL_PATH  = f'/opt/airflow/dags/data/{FILE_NAME}'

# 3-1-1. 오퍼레이터에 콜백함수 정의
def _check_s3_task(**kwargs):
    # S3Hook 이용 => 파일이 진짜로 존재하는지 리스트 출력
    # 1. S3Hook 이용 aws 엑세스함
    hook = S3Hook(aws_conn_id='aws_default')
    # 2. 훅을 이용하여 모든 키(파일명) 조회
    keys = hook.list_keys(bucket_name=BUCKET_NAME)
    # 3. 검출
    if not keys:
        raise ValueError("실패. 파일 업로드 실패0")
    # 파일이(키가) 실제 존재함
    for key in keys:
        if FILE_NAME in keys:
            logging.info('파일 업로드 되었음')
        else:
            logging.error('파일 업로드 실패1')
            raise ValueError("실패. 파일 업로드 실패2")

    pass

# 3. DAG 정의
with DAG(
    dag_id              = "09_aws_s3_basics",
    description         = "s3 연동 기본 연습",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@once',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['aws', 's3']
) as dag:
    # 3-1. 오퍼레이터 정의
    # 파일 생성
    create_file_task = BashOperator(
        task_id      = 'create_file_task',
        bash_command = f'echo "hello airflow & s3" > {LOCAL_PATH}'
    )
    # 로컬 파일 -> s3 업로드
    # LocalFilesystemToS3Operator : 설정만으로 업로드 가능함
    upload_to_s3_task = LocalFilesystemToS3Operator(
        task_id     = 'upload_to_s3_task',
        filename    = LOCAL_PATH,    # 원데이터가 저장되어 있는 위치+파일명
        dest_key    = FILE_NAME,     # 특정 데이터의 파일명
        dest_bucket = BUCKET_NAME,   # 파일이 업로드될 최종지(목적지) 버킷명
        aws_conn_id = 'aws_default', # aws 연결 정보 id, 대시보드상 등록한  id
        replace     = True           # 동일한 파일이 존재하면 덮어써라
    )
    # # s3 업로드 여부 확인
    check_s3_task = PythonOperator(
        task_id = 'check_s3_task',
        python_callable = _check_s3_task
    )
    # 3-2. 의존성(injection) -> 생략
    create_file_task >> upload_to_s3_task >> check_s3_task