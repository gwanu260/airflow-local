# 1. 모듈 가져오기
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import requests # MSA 서비스 호출 (HTTP 요청)
# mysql
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
# 추가
import random

# 2. 도커 상 네트워크 통해 접근할 API 주소
#    url: 서비스명 => ai-api-server
API_URL = "http://ai-api-server:8000/predict"

# 3. PythonOperator를 위한 함수
def _init_data_msa(**kwargs):
    # 실습 상황을 연출하기 위해 신용평가 점수 없는 신규 고객 데이터 강제로 추가
    # 실제는 특정 기간동안 가입한 고객(혹은 1주일(혹은 하루) 단위 가입한 고객) 조회
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득
    try:        
        with conn.cursor() as cursor:   # 커서 획득 
            # 1. 고객 테이블 생성
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    user_id VARCHAR(50) PRIMARY KEY,
                    income INT,
                    loan_amt INT,
                    credit_score INT DEFAULT NULL,
                    grade VARCHAR(10) DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # 2. 실습상 여러번 수행 가능하므로 -> 테이블내에 데이터 삭제(임시구성)
            cursor.execute('truncate table customers')
            # 3. 신규 고객 데이터 추가 (50명)
            params = [
                (
                    f'C{i:03d}',                 # 고객 아이디
                    random.randint(3000, 10000), # 소득
                    random.randint(1000, 5000),  # 대출, 론
                )
                for i in range(1, 51)
            ]
            # 4. 여러건의 데이터를 벌크 단위로 삽입
            sql = "insert into customers (user_id, income, loan_amt) values (%s,%s,%s)"
            cursor.executemany( sql, params )
            conn.commit()
            pass
    except Exception as e:
        logging.error(f'mysql에 데이터 삽입(적제) 중 오류 발생 {e}')
    finally:
        if conn:
            conn.close()
            logging.info('신규 고객 x명의 데이터 테이블 생성 및 입력 완료')
    pass

def _extract_data_msa(**kwargs):
    # 실습 => customers 테이블에서 신용평가점수가 없는 고객만 추출하여 
    # "ai_service_api_msa" task로 전달 (의존성 모두 풀어서 정상 구성)
    # Sql 구성 -> 쿼리 -> 결과를 [ {}, {}, ...] => 07_msa_..번에서 구성한대로 동일하게 태우면 됨
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    # 신용평가 점수가 없는 고객데이터만 조회->DataFrame 획득(통으로 획득(bulk read))
    df = mysql_hook.get_pandas_df('''
        select user_id, income, loan_amt from customers where credit_score is NULL;
    ''')
    # 결과셋 체크
    if df.empty:
        logging.info('신규 고객이 없습니다.')
        return []
    # 로그
    logging.info(f'신규 평가 대상 고객수 {len(df)}') # 실제는 데이터수가 제각각(규모에 따라 처리 방법 바뀔 수 있음)
    # 변환 : df -> [ {}, {}, ...]
    users = df.to_dict(orient='records')
    # 반환
    return users
    

def _ai_service_api_msa(**kwargs):
    # API 호출 (재료는 XCom 통해 통신)
    # 1. XCom  통해 신용평가를 수행하고자 하는 고객 데이터 획득
    ti         = kwargs['ti']
    users_data = ti.xcom_pull(task_ids='extract_data_msa')
    # 2. 외부에 존재하는 MSA(컨셉) API 호출(실제 연산은 외부에서 진행된다)
    try:
        res = requests.post( API_URL, json=users_data )
        #res.raise_for_status() # 200에 대한 점검 필요하면 진행(생략)
        results = res.json()    # 결과 획득
        logging.info(f'신용 평가 결과 획득 {results}')
        # 결과를 XCom 에서 획득 가는하게 반환
        return results
    except Exception as e:
        logging.error(f'API 호출 실패 {e}')
        raise
    pass

def _load_data_msa(**kwargs):
    # SQL 이용하여 DB 저장
    # 신용 평가 결과 획득 XCom 사용
    ti         = kwargs['ti']
    users_data = ti.xcom_pull(task_ids='ai_service_api_msa')
    if not users_data:
        logging.info("업데이트 할 데이터가 없음")
        return
    # SQL 저장
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득
    try:        
        with conn.cursor() as cursor:   # 커서 획득
            # 1. 데이터 update
            sql = '''
                update customers
                set credit_score = %s,grade=%s
                where user_id = %s
            '''
            params = [
                # 순서 조정 (쿼리문에 따라 위치 조저정됨)
                ( data['credit_score'], data['grade'], data['user_id'])
                for data in users_data
            ]
            cursor.executemany( sql, params)
            # 3. 커밋
            conn.commit()
            pass
    except Exception as e:
        logging.error(f'mysql에 데이터 업데이트(적제) 중 오류 발생 {e}')
    finally:
        if conn:
            conn.close()
            logging.info('mysql에 데이터 업데이트(적제) 완료')
    pass

# 4. DAG 구성
with DAG(
    dag_id              = "07_batch_inference_v1",
    description         = "msa 서비스 호출하여 스케줄링",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['msa', 'fastapi','batch_inference']
) as dag:
    # Task
    # 1. 더미 데이터  준비 (실습상 편의적으로 세팅, 실제는 실 서비스에서 저장됨)
    init_data    = PythonOperator(
        task_id     = "init_data_msa",
        python_callable = _init_data_msa
    )
    # 2. 신용평가 미처리 고객 데이터 추출
    extract_data    = PythonOperator(
        task_id     = "extract_data_msa",
        python_callable = _extract_data_msa
    )
    # 3. AI 서비스 호출-> 신용평가 결과 획득
    ai_service_api  = PythonOperator(
        task_id     = "ai_service_api_msa",
        python_callable = _ai_service_api_msa
    )
    # 4. 결과를 저장
    load_data       = PythonOperator(
        task_id     = "load_data_msa",
        python_callable = _load_data_msa
    )

    # 의존성 1->2->3->4
    init_data >> extract_data >> ai_service_api >> load_data
    pass