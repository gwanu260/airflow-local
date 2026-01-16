# B팀 작성
# 잠복하여 감시하던중 업로드되는 것을 자동 감지(S3KeySensor) -> 파일 읽고 -> 처리 -> 삭제
# 버킷내 income 폴더 하위는 임시로 사용하는 공간 (늘 채워져 있지 않다!!)
# 1. 모듈 가져오기
from datetime import datetime, timedelta 
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # 읽기용
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # 감시(지)용
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 삭제용

# 2. 상수, 환경변수, 고정값
# 버킷명
BUCKET_NAME = 'airflow-ai-en-4'
# 최종 버킷내 특정 폴더에 생성될 파일명 -> KEY
S3_KEY      = 'income/trigger_data.csv'

def _processing_data(**kwargs):
    # 감시 대상이 포착되며 해당 내용 읽어서 로그 출력
    hook = S3Hook(aws_conn_id='aws_default') # 연결
    # 파일 내용 읽기
    data = hook.read_key(key=S3_KEY, bucket_name=BUCKET_NAME)
    # 로그출력
    logging.info('--- 로그 출력 시작 ---')
    logging.info(data)
    logging.info('--- 로그 출력 종료 ---')
    pass

# 3. DAG 정의
with DAG(
    dag_id              = "09_s3_consumer_v1",
    description         = "s3의 특정 버킷에 데이터 변화 감지->읽기->삭제",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily', # 스위치 켜면 작동 => 센서감지 않되면 -> 대기중(연두색)
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['aws', 's3', 'consumer']
) as dag:
    # 3-1. 오퍼레이터
    # 감시자(감지)
    waitting_trigger = S3KeySensor(
        task_id = 'waitting_trigger',
        # 감시 대상 설정
        bucket_key  = S3_KEY,         # 버킷 내 타겟
        bucket_name = BUCKET_NAME,    # 버킷 이름
        aws_conn_id = 'aws_default',  # 접속 정보
        # 감시 설정(방법)
        mode          = "reschedule",  # 대기중에는 자원 반납
        poke_interval = 10,  # 10초 간격으로 체크(실습상 부여, 실제는 다를 수 있음)
        timeout       = 600 # 60*10 => 서비스 가동후 10분 넘게 감지가 않되면 종료
    )
    # 데이터 읽기(처리)
    processing_data  = PythonOperator(
        task_id = 'processing_data',
        python_callable = _processing_data
    )
    # 파일 삭제(s3 특정키 삭제)
    delete_data      = S3DeleteObjectsOperator(
        task_id = 'delete_data',
        bucket  = BUCKET_NAME,    # 버킷 이름
        keys    = [S3_KEY],       # 버킷 내 타겟들을 n개 지정
        aws_conn_id = 'aws_default',  # 접속 정보
    )

    # 3-2. 의존성
    waitting_trigger >> processing_data >> delete_data