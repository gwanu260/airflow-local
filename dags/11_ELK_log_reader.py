'''
ELK중 E(OpenSearch)만 현재 사용중, K는 대시보드, L는 미사용
- OpenSearch 접속 => 검색 => 결과 획득 
=> 집계 분석(Athean or redshift or [v]pandas os spark(EMR))
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
from opensearchpy import OpenSearch
from airflow.hooks.base import BaseHook
import pendulum

# 2. 환경변수
# - Admin -> Connections에 'opensearch_default'가 설정되어 있어야 함
conn       = BaseHook.get_connection('opensearch_default')
HOST       = conn.host
AUTH       = (conn.login, conn.password)
INDEX_NAME = 'a-factory-45x-senser-v1'

# 3-2. 실제 검색 -> 분석 함수
def _analysis_task(**kwargs): 
    # 1. 검색엔진 접속
    logging.info('검색엔진 접속')
    client = OpenSearch(
        hosts           = [ { "host": HOST, "port": 443 } ],
        http_auth       = AUTH,
        use_ssl         = True,
        verify_certs    = True      
    )  
    
    # 2. 쿼리(질의) -> 최근 10분내 데이터 가져오기
    logging.info('쿼리(질의) 구성')
    query = {
        "size": 1000, 
        "query": {
            "range": {
                "timestamp": {
                    "gte": "now-10m"
                }
            }
        }
    }  

    # 3. 결과 획득 및 체크
    logging.info('결과 획득 중...')
    res = client.search(index=INDEX_NAME, body=query)
    hits = res['hits']['hits']
    
    if not hits:
        logging.info('조회된 건수가 없습니다.')
        return
    else:
        logging.info(f'조회된 건수: {len(hits)}건')

    # 4. 전처리 : List of Dict -> DataFrame
    logging.info('데이터 전처리(Pandas)')
    searching_data = [hit['_source'] for hit in hits] 
    df = pd.DataFrame(searching_data)

    # 5. 분석 : 오븐별 집계
    logging.info('데이터 집계 분석')
    # oven_id 컬럼명이 실제 데이터와 일치하는지 확인 필요
    analysis = df.groupby('oven_id').agg({
        'temperature': 'mean',
        'vibration': 'max',
        'status': 'count'
    })
    print(analysis) 

    # 6. 이상치 탐지 (고온 230이상 혹은 DANGER 상태 체킹)
    logging.info('이상 탐지 로직 실행')
    outlier = df[df['status'] == 'DANGER']
    
    if not outlier.empty:
        logging.warning('!!! 이상 탐지됨 !!!')
        logging.warning('오븐 온도가 너무 높음 -> 오버쿠킹 위험 -> 이 시간대 생산품 불량 가능성 농후')
    
    # 작업 완료 후 필요 시 pass 생략 가능

# 3. DAG 정의
with DAG(
    dag_id              = "11_ELK_log_reader_v1",
    description         = "OpenSearch 검색 및 센서 데이터 분석 파이프라인",
    default_args        = {
        'owner': 'de_1team_manager',        
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval   = '*/5 * * * *', # 5분 간격 실행
    start_date          = pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup             = False,
    tags                = ['elk', 'opensearch', 'analysis', 'reader']
) as dag:

    # 3-1. 오퍼레이터 정의
    analysis_task = PythonOperator(
        task_id         = 'analysis_task',
        python_callable = _analysis_task
    )