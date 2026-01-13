# 파이썬 로직을 task로 사용
# task간 통신을 통해서 상호 대화

from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging # 레벨별로 로그 출력 가능 (에러, 경고, 디버깅, 정보 등등 레벨 지정)

def _extract_cb(**kwargs):
    '''
    ETL에서 Extract 담당    
    :param kwargs: Airflow가 작업 실행하기 직전에 정보를 injection(주입) 해주는 위치
                   해당 내용은 Airflow 내부의 context(딕셔너리 타입)라는 공간에 정보들이 저장되어 있음
                   Task 내부에서 Airflow의 정보들을 접근 및 사용 가능함
    '''
    # 1. airflow가 주입한 airflow context에서 필요한 정보 추출
    # 'ti': <TaskInstance: ...> =>현재 작동중인 TaskInstance 객체를 의미함 (대시보드상 사각박스)
    ti           = kwargs['ti']
    # 'ds': '2025-01-01', 'ds_nodash': '20250101' 
    #  => 이 작업을 수행하기로 스케줄링된 논리적인 날짜
    execute_date = kwargs['ds']
    # 'run_id': 'scheduled__2025-01-01T00:00:00+00:00'
    #  => 이번 실행의 고유한 ID => 로그 추적용
    run_id       = kwargs['run_id']

    # 2. task 본연 업무 구현    
    # print( kwargs, type(kwargs), kwargs.keys() )
    # 여기서는 로그만 남김
    logging.info('== Extract 작업 시작 ==')
    logging.info(f'작업시간 {execute_date} , 실행 ID {run_id}')
    logging.info('== Extract 작업 끝 ==')

    # 3. 필요시 XCom을 통해서 특정 데이터를 다은 task로 전달함
    #    return 데이터 => 자동으로 XCom에 push 처리됨 (게시판에 글 등록됨)
    return "Data_Extract_성공"

def _transform_cb(**kwargs):
    '''
    ETL에서 Transform 담당    
    :param kwargs: 다양한 매개변수가 접근됨
    '''
    # 1. ti 객체 획득
    ti = kwargs['ti']

    # 2. task 본연 업무 구현
    # extract_task에서 전달한 데이터 획득
    data = ti.xcom_pull(task_ids='extract_data_task')
    # print( kwargs, type(kwargs), kwargs.keys() )
    logging.info('== Transform 작업 시작 ==')
    logging.info( f"Data_transform_성공 {data}" )
    logging.info('== Transform 작업 시작 ==')
    
    #return "Data_transform_성공"
    pass

with DAG(
    dag_id              = "02_basics_python_v1",
    description         = "파이썬 task 구성 및 통신(xcom)",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },
    schedule_interval   = '@once', # 수동으로 딱 한번 수행, 주기성 없음
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['python','xcom', 'context']
) as dag:
    # 1. task 정의 (ETL을 고려하여서 네이밍)
    extract_task   = PythonOperator(
        task_id         = "extract_data_task",
        python_callable = _extract_cb
    )
    transform_task = PythonOperator(
        task_id         = "transform_data_task",
        python_callable = _transform_cb
    )
    # 2. 의존성 (순서 작성)
    extract_task >> transform_task
    # [리뷰실습] : load_task 구현하시오. 
    #             의존성은 transform_task 실행 성공후 task 수행되게 구성
    #             주 업무 : transform_task에서 리턴한 값(문자열)을 받아서 로그 출력
    pass