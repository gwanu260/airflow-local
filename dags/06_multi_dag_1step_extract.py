# TriggerDagRunOperator : 트리거를 직접 발동 시켜서 1번이 완료되면 2번 DAG을 실행시킴
from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 핵심
import logging
import json
import random
import os

DATA_PATH = '/opt/airflow/dags/data' # 도커 내부에 있는 서비스(리눅스기반)의 특정경로
os.makedirs( DATA_PATH , exist_ok=True)

def _extract_data_sensor(**kwargs):
    # kwargs <- airflow context 정보가 전달됨
    # 스마트팩토리에 설치된 온도 센서 데이터가 어딘가(데이터 레이크:s3)에 쌓이고 있다 -> 추출해서 가져온다
    # 여기서는 더미로 구성
    data = [ {
        "sensor_id"  : f"SENSOR_{i+1}", # 장비 ID
        "timestamp"  : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # YYYY-MM-DD hh:mm:ss
        "temperature": round( random.uniform(20.0, 150.0), 2),
        "status"     : "on", # on/off
    } for i in range(10) ]

    # 더미로 만든 데이터를 파일로 저장 => /opt/airflow/dags/data/sensor_data_DAG수행날짜.json
    file_path = f'{DATA_PATH}/sensor_raw_data_{ kwargs["ds_nodash"] }.json'
    with open(file_path, 'w') as f:
        json.dump( data , f)
    
    # XCom을 통해서 다음 task에서 접근 가능함
    # 다음 테스크에게 무엇을 전달할 것인가? 
    # 1) 데이터(저용량일때 가볍게, 사용 자제) 2) [v]파일경로(로컬, s3경로등)
    logging.info(f'extract 완료 데이터={file_path}')
    return file_path

with DAG(
    dag_id              = "06_multi_dag_1step_extract",
    description         = "extract 전용 DAG",
    default_args        = {
        'owner'          :'de_1team_manager',
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['extract', 'etl']
) as dag:
    # 오퍼레이터
    extract_data    = PythonOperator(
        task_id     = "extract_data_sensor",
        python_callable = _extract_data_sensor
    )
    # 다음 DAG을 실행시키기 위해 트리거 발동
    trigger_transform = TriggerDagRunOperator(
        task_id         = "trigger_transform",
        # 트리거의 대상 -> 어던 DAG을 구동시킬것인가
        trigger_dag_id  = "06_multi_dag_2step_transform", 
        # 전달할 데이터(json의 경로)
        # JINJA 템플릿 엔진을 통해 XCOM의 전달된 데이터를 동적 세팅
        conf            = { "json_path":"{{ task_instance.xcom_pull(task_ids='extract_data_sensor') }}"},
        # DAG의 실행시간을 리셋 => 동일하게 맞춘다
        # TriggerDagRunOperator의 수행시간을 
        # PythonOperator(바로 위에 있는. 혹은 직전)의 수행시간에 맞춤
        reset_dag_run   = True, 
        wait_for_completion = False # 수행되면 대기 없이 그대로 종료(비동기)
    )

    # 의존성
    extract_data >> trigger_transform