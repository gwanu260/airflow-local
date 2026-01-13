# ETL을 통해서 trainning_data.csv 생성

# 1. 필요한 모듈 가져오기
from datetime import datetime, timedelta 
import logging
import random
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset # 2.4 이후 지원 데이터셋 모듈

# 2. 데이터 저장 경로 지정 (도커내 리눅스 내부 경로 지정)
DATA_PATH='/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True) # 먹통성 고려(여러번 수행되도 동일한 결과 나오도록)

# 3. 데이터셋 (Airflow가 감시하는(바라보는) -> 트리거 발동시킴) 정의 => URI 형태
#    최종 task의 결과로 trainning_data.csv 만들어지면 다음 DAG 작동됨
trigger_dataset = Dataset(f'file://{DATA_PATH}/trainning_data.csv')

# 4-1-1. 오퍼레이터에 콜백함수 정의
def _etl_task_generator(**kwargs):
    # 학습용 더미 데이터 생성 (Feature/label(target) 생성)
    data = list()
    for i in range(100):
        income   = random.randint( 2000, 10000 )
        loan_amt = random.randint( 100,  5000  )
        data.append({
            "income"  : income,
            "loan_amt": loan_amt,
            # 1 : 대출 승인, 0 : 대출 불허(거절)
            "target"  : 1 if income > loan_amt else 0
        })
    # [ {}, {}, ] -> DataFrame -> csv, 코드 완성, 로그 출력(학습 데이터 완료: 경로)
    df        = pd.DataFrame(data)
    save_path = f'{DATA_PATH}/trainning_data.csv'
    df.to_csv( save_path, index=False )
    logging.info(f'학습 데이터 완료: {save_path}')
    # trainning_data_년월일.csv <= 이렇게 구성할 경우 별도 삭제 않하면 데이터가 쌓일수 있음

    pass

# 4. DAG 정의
with DAG(
    dag_id              = "08_etl_v1",
    description         = "etl 서비스",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['etl', 'part1']
) as dag:
    # 4-1. 오퍼레이터 정의
    etl_task = PythonOperator(
        task_id = 'etl_task_generator',
        python_callable = _etl_task_generator,
        # 핵심은 Dataset 연결
        # 해당 테스크가 종료되면 -> trigger_dataset이 업데이트 되었다고 공지함
        # 다음 DAG의 스케쥴이 시간이 아닌 trigger_dataset을 지정(바라보면) -> 해당 공지(트리거)
        # 바로 연쇄적으로 다음 DAG 작동 (비동기적 작동)
        outlets = [trigger_dataset]
    )
    # 4-2. 의존성(injection) -> 생략