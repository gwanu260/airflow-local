# 1. 모듈 가져오기
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import requests # MSA 서비스 호출 (HTTP 요청)
# mysql
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 2. 도커 상 네트워크 통해 접근할 API 주소
#    url: 서비스명 => ai-api-server
API_URL = "http://ai-api-server:8000/predict"

# 3. PythonOperator를 위한 함수
def _extract_data_msa(**kwargs):
    # 더미 데미터 수동 구성 -> (업그레이드) sql 쿼리에서 조회
    users = [
        { "user_id":"C001", "income": 5000, "loan_amt": 2000 },
        { "user_id":"C002", "income": 3000, "loan_amt": 5000 },
        { "user_id":"C003", "income": 8000, "loan_amt": 1000 }
    ]
    # 차후에는 고객 테이블 구성-> 더미로 입력(신용평가 여부 랜덤구성)
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
        logging.error("신용 평가 결과 없음")
        raise ValueError('신용 평가 결과 없음')
    # SQL 저장
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득
    try:        
        with conn.cursor() as cursor:   # 커서 획득
            # 1. 테이블이 없다면 생성(create)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS credit_scores (                    
                    user_id VARCHAR(50),                    
                    credit_score INT,
                    grade VARCHAR(4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            # 2. 데이터 적재(insert)
            sql = '''
                insert into credit_scores 
                (user_id, credit_score, grade)
                values
                (%s, %s, %s)
            '''
            cursor.executemany( sql, 
                [ ( user['user_id'], user['credit_score'], user['grade'] ) for user in users_data ]
            )
            # 3. 커밋
            conn.commit()
    except Exception as e:
        logging.error(f'mysql에 데이터 삽입(적제) 중 오류 발생 {e}')
    finally:
        if conn:
            conn.close()
            logging.info('mysql에 데이터 삽입(적제) 완료')
    pass

# 4. DAG 구성
with DAG(
    dag_id              = "07_msa_pattern_v1",
    description         = "msa 서비스 호출하여 스케줄링",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['msa', 'fastapi']
) as dag:
    # Task
    # 1. 더미 데이터  준비
    extract_data    = PythonOperator(
        task_id     = "extract_data_msa",
        python_callable = _extract_data_msa
    )
    # 2. AI 서비스 호출-> 신용평가 결과 획득
    ai_service_api    = PythonOperator(
        task_id     = "ai_service_api_msa",
        python_callable = _ai_service_api_msa
    )
    # 3. 결과를 저장
    load_data    = PythonOperator(
        task_id     = "load_data_msa",
        python_callable = _load_data_msa
    )

    # 의존성 1->2->3
    extract_data >> ai_service_api >> load_data
    pass