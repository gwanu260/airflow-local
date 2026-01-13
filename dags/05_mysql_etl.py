from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
# mysql
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 데이터
import json
import random
import pandas as pd
import os

# 프로젝트 폴더 내부에 /dags/data에 내부에서 TASK 과정에 생성되는 파일을 동기화하여 확인할수 있게
# 위치를 선정
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
    file_path = f'{DATA_PATH}/sensor_data_{ kwargs["ds_nodash"] }.json'
    with open(file_path, 'w') as f:
        json.dump( data , f)
    
    # XCom을 통해서 다음 task에서 접근 가능함
    # 다음 테스크에게 무엇을 전달할 것인가? 
    # 1) 데이터(저용량일때 가볍게, 사용 자제) 2) [v]파일경로(로컬, s3경로등)
    logging.info(f'extract 완료 데이터={file_path}')
    return file_path
    pass
def _transform_data_std_change(**kwargs):
    # 1. extract_data_sensor task에서 전달한 file_path 획득(XCom 이용)
    ti             = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='extract_data_sensor')
    
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
    pass
def _load_data_mysql(**kwargs):
    # csv => mysql, 이를 위해서 MySqlHook을 사용
    # 1. csv 경로 획득
    ti             = kwargs['ti']
    csv_file_path  = ti.xcom_pull(task_ids='transform_data_std_change')

    # 1.5 csv -> df
    df = pd.read_csv( csv_file_path )

    # 2. 연결 -> I/O (예외처리, with문 -> auto close() 처리)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득
    try:        
        with conn.cursor() as cursor:   # 커서 획득
            # 1. 쿼리문 준비
            sql = '''
                insert into sensor_readings 
                (sensor_id, timestamp, temperature_c, temperature_f)
                values (%s, %s, %s, %s)
            '''
            # 2. 데이터별, 컬럼별 추출하여 쿼리 수행 ( executemany() )
            #params = list() # [ (값, 값, 값, 값), (), () ]
            params = [
                ( data['sensor_id'],     data['timestamp'], 
                  data['temperature'], data['temperature_f'] )
                for _, data in df.iterrows()
            ]
            logging.info(f'파라미터 {params}')
            cursor.executemany( sql, params )
            # 3. 커밋
            conn.commit()
            logging.info('mysql에 데이터 삽입(적제) 성공(success)')
    except Exception as e:
        logging.error(f'mysql에 데이터 삽입(적제) 중 오류 발생 {e}')
    finally:
        if conn:
            conn.close()
            logging.info('mysql에 데이터 삽입(적제) 완료')
    pass

# DAG
with DAG(
    dag_id              = "05_mysql_etl_v1",
    description         = "etl 수행하여 mysql에 적제",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['mysql', 'etl']
) as dag:
    # 오퍼레이터(task)
    create_table    = MySqlOperator(
        task_id     = "create_table",
        mysql_conn_id = 'mysql_default', # UI>Admin>connections>등록된 내용과 동일하게 구성
        # 여러번 수행할수 있으므로 IF NOT EXISTS => 존재하지 않으면 생성
        # 기재하지 않으면 2번째 주기에서  task 구동시 실패 발생할 수 있음
        # sql을 등록하시면 task instance 수행시 반드시 쿼리가 작동함
        sql         = '''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(50),
                timestamp DATETIME,
                temperature_c FLOAT,
                temperature_f FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

        '''
    )
    extract_data    = PythonOperator(
        task_id     = "extract_data_sensor",
        python_callable = _extract_data_sensor
    )
    transform_data  = PythonOperator(
        task_id     = "transform_data_std_change",
        python_callable = _transform_data_std_change
    )
    load_data       = PythonOperator(
        task_id     = "load_data_mysql",
        python_callable = _load_data_mysql
    )

    # 의존성
    # 테이블 생성 >> Extract >> transform >> Load
    create_table >> extract_data >> transform_data >> load_data