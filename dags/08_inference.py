# model_artifact.json -> msa 기반 api 서비스 사용
# 향후 MLOps의 대표적 인 도구인 MLFLow을 적용 -> 모델 생성후 엔드포인트 구성 가능해짐
# API는 동일한데 모델이 교체됨(v1->v2->v3 ->... vn) => 코드 수정 없음
# 여기서는 새로운 모델이 발견되었다 => 모델이 교체되었다(가정 -> mlflow 관련 DAG 구성 필요) 가정

# 새로 업데이트된 기준(모델)에 맞춰 대상 고객의 신용평가를 새로(혹은 업데이트(확장성고려)) 진행

# 1. 모듈 가져오기
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import requests # MSA 서비스 호출 (HTTP 요청)
# mysql
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.datasets import Dataset


# 2. 상수(처럼 관리, 고정값) 정의
#    inference 수행할 msa기반 url
API_URL   = "http://ai-api-server:8000/predict"
#    데이터셋
DATA_PATH ='/opt/airflow/dags/data'

# 3. 데이터셋 (감시해야 하는)
model_dataset   = Dataset(f'file://{DATA_PATH}/model_v1.json')

# 3. PythonOperator를 위한 함수
def _extract_data_msa(**kwargs):
    # 더미 데미터 수동 구성 -> (업그레이드) sql 쿼리에서 조회
    users = [
        { "user_id":"C101", "income": 6000, "loan_amt": 2000 },
        { "user_id":"C102", "income": 2000, "loan_amt": 6000 },
        { "user_id":"C103", "income": 9000, "loan_amt": 1000 }
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
            params = [ 
                ( data['user_id'], data['credit_score'], data['grade'])
                for data in users_data
            ]
            cursor.executemany( sql, params)
            # 3. 커밋
            conn.commit()
            pass
    except Exception as e:
        logging.error(f'mysql에 데이터 삽입(적제) 중 오류 발생 {e}')
    finally:
        if conn:
            conn.close()
            logging.info('mysql에 데이터 삽입(적제) 완료')
    pass

# 4. DAG 구성
with DAG(
    dag_id              = "08_inference_v1",
    description         = "msa 서비스 호출 모델 추론 요청",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule            = [model_dataset],
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['inference', 'part3']
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