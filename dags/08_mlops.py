# trainning_data.csv -> 학습(시뮬레이션) -> 모델덤프(model_artifact.json)
# DAG에서 아래 와 같은 과정을 진행했다고 가정하고 시뮬레이션 진행


# 1. 필요한 모듈 가져오기
from datetime import datetime, timedelta 
import logging
import random
import os
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

# 2. 공통(모든 DAG)
DATA_PATH='/opt/airflow/dags/data'

# 3. 데이터셋 (감시해야 하는, 생성해야 하는)
trigger_dataset = Dataset(f'file://{DATA_PATH}/trainning_data.csv') # 감시
model_dataset   = Dataset(f'file://{DATA_PATH}/model_v1.json')      # 생성

# 4-1-1. 오퍼레이터에 콜백함수 정의
def _train_task_dump(**kwargs):
    # 1. 모델 학습
    # 일반 모델 학습 과정(큰 포인트만 표시)
    # df = pd.read_csv(...)
    # 데이터 분할
    # ML 학습 => 실제로는 모델이 계속 업그레이드(기존 가중치유지->신규 데이터 학습) 되어야함
    #         => 전이학습(transfer learning):원래 목적(업스트림)과 실제 사용(다운스트림) 목적 다르게 구성
    #         => 여기서는 동일 데이터로 계속 가중치를 업데이트하는 전략 활용
    #         => 데이터는 매일, 매시간 계속 쌓인다는 전제하에서 진행(실시간, 특정단위(배치) 컨셉)
    logging.info('ML/DL/LLM 등 모델 학습 중.. epoch 1, epoch 2, ... epoch n') # 연출(학습했다고 가정)

    # 2. 모델 저장(임시:메타 정보만, 아티펙트만 저장한다)
    model_info = {
        'version'  : '1.0',
        'accuracy' : 0.96,
        'train_at' : datetime.now().isoformat()
    }
    # 3. 저장
    save_path = f'{DATA_PATH}/model_v1.json'
    with open(save_path, 'w') as f:
        json.dump(model_info, f)
    logging.info(f'모델 학습 완료 -> 저장 {save_path}')

# 4. DAG 정의
with DAG(
    dag_id              = "08_mlops_v1",
    description         = "mlops 중 모델 학습",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    # 핵심 : trigger_dataset이 생성되어야 해당 DAG이 작동됨 (조건은 시간이 아니라 데이터셋임)
    schedule            = [trigger_dataset],
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['mlops', 'train', 'part2']
) as dag:
    # 4-1. 오퍼레이터 정의
    train_task = PythonOperator(
        task_id         = 'train_task_dump',
        python_callable = _train_task_dump,
        # 핵심 : task의 결론, 새로운 모델이 나왔다고 공지
        outlets         = [model_dataset]
    )
    # 4-2. 의존성(injection) -> 생략