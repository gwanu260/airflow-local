# Athena의 쿼리가 완료되었다 라는 시그널을 감지(시) 하고 싶다면 사용 => AthenaSensor
# 앞선 쿼리 작업이 오래 걸릴때 센서를 붙여서 언젠가 끝나면 바로 다음 task 작동되게 시점 체크할수 있음

# 1. 모듈 가져오기
from datetime import datetime, timedelta 
import logging
from airflow import DAG
# 아테나 대상으로 오퍼레이터 작업, 센서
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

# 2. 환경변수(상수값), s3 버킷등 경로
BUCKET_NAME   = 'airflow-ai-en-4'
ATHENA_DB     = 'de_4'
S3_OUTPUT_LOC = f's3://{BUCKET_NAME}/athena-outputs/' # 결과물 저장 위치

# 3. DAG 정의
with DAG(
    dag_id              = "10_aws_athena_sensor_v1",
    description         = "athena 오퍼레이터 작업 완료에 대한 감지",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = None, # 수동실행 (트리거)
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['aws', 'athena','sensor']
) as dag:
    # 3-1. 아테나 쿼리 실행 (80 이상 점수를 받은 학생 명단 조회)
    run_query_task  = AthenaOperator(
        task_id         = "run_query_task",
        query           = "select * from s3_exam_csv where score >= 80",
        database        = ATHENA_DB,
        output_location = S3_OUTPUT_LOC,
        aws_conn_id     = 'aws_default',
        # 쿼리 실행 아이디 => query_execution_id를 반환 => XCom을 통해 다른 테스크에서 획득가능
        do_xcom_push    = True
    )

    # 3-2. 아테나 센서 (쿼리 상태를 감지 -> 신호가 오면 다음 작업 전개되게 구성)
    #      비동기 적인 상황 -> 쿼리 수행시간이 제각각임( 서비리스, ATHENA 자체 처리 상황 )
    #      TASK -> ATHENA 작업 -> 센서배치 -> 신호가 감지되면 다음 작업 전개되게 구성
    sensor_task = AthenaSensor(
        task_id            = "sensor_task",
        # XCom을 통해서 테스트 출력값 세팅 -> 세팅된다 -> 감지되었다 -> 다은 단계 진행할수 있다
        # 앞에 코드 대비 장시간 대기할때 유용함
        query_execution_id = "{{ task_instance.xcom_pull(task_ids='run_query_task') }}",
        # 센서는 감시 활동 진행해야함
        poke_interval      = 10,   # 10초 마다 감지
        timeout            = 600,  # 최대 대기 시간 - 10분
        aws_conn_id        = 'aws_default'
    )
    
    # 의존성
    run_query_task >> sensor_task