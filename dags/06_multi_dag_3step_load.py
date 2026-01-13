from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 핵심
import logging
import os
import pandas as pd
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

def _load_data_mysql(**kwargs):
    # csv => mysql, 이를 위해서 MySqlHook을 사용
    # 1. csv 경로 획득
    csv_file_path  = kwargs['dag_run'].conf.get('csv_path')
    if not csv_file_path:
        # 예외 발생 => 실패 처리
        logging.error('2step에서 conf를 통해 데이터 전달(획득) 실패')
        raise ValueError('2step에서 conf를 통해 데이터 전달(획득) 실패')

    # 1.5 csv -> df
    df = pd.read_csv( csv_file_path )

    # 2. 연결 -> I/O (예외처리, with문 -> auto close() 처리)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득
    try:        
        with conn.cursor() as cursor:   # 커서 획득
            # 0. 테이블 생성 -> MySqlOperator에서 진행 X 직접 TASK에서 합병(컨셉)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_readings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    sensor_id VARCHAR(50),
                    timestamp DATETIME,
                    temperature_c FLOAT,
                    temperature_f FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

            ''')
            
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
        raise ValueError('mysql에 데이터 삽입(적제) 중 오류 발생')
    finally:
        if conn:
            conn.close()
            logging.info('mysql에 데이터 삽입(적제) 완료')
    pass

with DAG(
    dag_id              = "06_multi_dag_3step_load",
    description         = "load 전용 DAG",
    default_args        = {
        'owner'          :'de_1team_manager',
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['load', 'etl']    
) as dag:
    load_data       = PythonOperator(
        task_id     = "load_data_mysql",
        python_callable = _load_data_mysql
    )
