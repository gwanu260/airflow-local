from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 핵심
import logging
import os
import pandas as pd

# 실습!! 구현하시오
DATA_PATH = '/opt/airflow/dags/data'

def _transform_data_std_change(**kwargs):
    # 1. extract_data_sensor task에서 전달한 file_path 획득(다른 DAG에서 전달한 conf를 이용)
    dag_run        = kwargs['dag_run']
    # 멀티 DAG에서 데이터 통신하기(수신)
    json_file_path = dag_run.conf.get('json_path')
    if not json_file_path:
        # 예외 발생 => 실패 처리
        logging.error('1step에서 conf를 통해 데이터 전달(획득) 실패')
        raise ValueError('1step에서 conf를 통해 데이터 전달(획득) 실패')
    
    # 2. 해당 데이터를 DataFrame으로 로드 (json -> df)
    df = pd.read_json( json_file_path ) # 경로에 맞춰서 알아서 데이터를 로드

    # 3. 전처리 수행 -> 섭씨를 화씨로 변환 처리 ( 화씨(°F) = (섭씨(°C) × 9/5) + 32 )
    #    컨셉 => 우리 공장에서는 측정온도를 섭씨 100도 이하만 정상 데이터로 간주한다 
    #           (100도 이상 온도는 이상치로 간주) => 이상치 제거 or 100도 이하만 추출
    #           => 블리언 인덱싱 : df[ 조건식 ]
    target_df = df[ df['temperature'] < 100 ].copy() # deep=True) # 데이터 크기 따라 선택
    #    파생변수로 화씨 데이터로 구성 -> 'temperature_f'
    target_df['temperature_f'] = (target_df['temperature'] * 9/5) + 32

    # 4. 전처리된 데이터를 저장 => 동일 공간에 파일명만 preprocessing_data_{ds_nodash}.csv
    file_path = f'{DATA_PATH}/preprocessing_data_{ kwargs["ds_nodash"] }.csv'
    target_df.to_csv( file_path, index=False)
    logging.info(f'transform 데이터 전처리후 csv 저장 완료 {file_path}')

    # 5. 저장된 csv 경로를 다음 task에서 사용할 수 있게 반환 처리 (df -> csv, 인덱스제거)
    return file_path

with DAG(
    dag_id              = "06_multi_dag_2step_transform",
    description         = "transform 전용 DAG",
    default_args        = {
        'owner'          :'de_1team_manager',
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['transform', 'etl']    
) as dag:
    transform_data  = PythonOperator(
        task_id     = "transform_data_std_change",
        python_callable = _transform_data_std_change
    )
    # 다음 DAG을 실행시키기 위해 트리거 발동
    trigger_load = TriggerDagRunOperator(
        task_id         = "trigger_load",
        # 트리거의 대상 -> 어던 DAG을 구동시킬것인가
        trigger_dag_id  = "06_multi_dag_3step_load", 
        # 전달할 데이터(json의 경로)
        # JINJA 템플릿 엔진을 통해 XCOM의 전달된 데이터를 동적 세팅
        conf            = { "csv_path":"{{ task_instance.xcom_pull(task_ids='transform_data_std_change') }}"},
        # DAG의 실행시간을 리셋 => 동일하게 맞춘다
        # TriggerDagRunOperator의 수행시간을 
        # PythonOperator(바로 위에 있는. 혹은 직전)의 수행시간에 맞춤
        reset_dag_run   = True, 
        wait_for_completion = False # 수행되면 대기 없이 그대로 종료(비동기)
    )

    # 의존성
    transform_data >> trigger_load