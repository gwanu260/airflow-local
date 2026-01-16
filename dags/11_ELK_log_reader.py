'''
ELK중 E(OpenSearch)만 현재 사용중, K는 대시보드, L는 미사용
OpenSearch 접속 => 검색 => 결과 획득 => 분속(Athean or redshift or [v]pandas os spark(EMR))
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
from opensearchpy import OpenSearch
from airflow.hooks.base import BaseHook

# 2. 환경변수
conn = BaseHook.get_connection('opensearch_default')
HOST        = conn.host
AUTH        = (conn.login, conn.password)
INDEX_NAME  = 'a-factory-45x-sensor-v1'

# 3-2. 실제 검색 -> 분석
def _analysis_task(**kwargs): # (**context), () 다양하게 매개변수를 표시함
    # 1. 검색엔진 접속
    client = OpenSearch(
        hosts               = [ { "host":HOST, "port":443 } ],
        http_compress       = True,
        http_auth           = AUTH,
        verify_certs        = True,
    )
    logging.info('검색엔진 접속')
    # 2. 쿼리(질의) -> 최근 10분내 데이터 가져오기
    logging.info('쿼리(질의)')
    # 질의 방식 -> 별도로 opensearch 에 적합한 형태는 본적 없음 -> 제시함
    query = {
        "size": 1000, # 1000개 문서만 가져와라
        "query":{
            "range":{
                "timestamp":{
                    "gte": "now-10m"   # 최근 10분이내 데이터 1000개 까지만 가져오기
                }
            }
        }
    }
    
    # 3. 결과 획득 및 체크(검색 결과 x)
    logging.info('결과 획득')
    res = client.search(index=INDEX_NAME, body=query)
    # 히트수 획득 -> 건수
    hits = res['hits']['hits']
    if not hits:
        print('조회된 건수가 없다')
        return
    else:
        print(f'조회된 건수 있다{ len(hits) }')
    # 조회결과를 -> s3 보냄(중간 결과물)
    # -----------------------------------------
    # 4. 전처리 : res(or s3) -> df
    logging.info('전처리')
    searching_data = [ hits['_source'] for hit in hits] # [ {}, {}, {}, ...]
    df = pd.DataFrame( searching_data )
    
    # 5. 분석
    logging.info('분석')
    # 오븐별(센서`별`)=>집계/그룹화
    analysis = df.groupby('ovenid').agg({
        'temperature':'mean',
        'vibration':'max',
        'status':'count'
    })
    print( analysis ) # s3 업로드( 중간 결과물 )
    
    # 6. 분석 결과 혹은 이상치 탐지(고온 230이상 위험 시그널 체킹)
    logging.info('이상탐지')
    # status 컬럼이 DANGER 였던 건수 출력
    outlier = df[ df['status'] == 'DANGER' ]
    if not outlier.empty :
        logging.info('이상 탐지됨 : 오븐 온도가 너무 높음!! -> 오버쿠킹 -> 맛의 일반성을 훼손 -> 이시간대 생산품 불량')
        pass

# 3. DAG정의
with DAG(
    dag_id      = "11_ELK_log_generator_v1",
    description = "OpenSearch에게 검색(질의) -> 결과획득 -> 분석",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },
    schedule_interval   = '*/5 * * * *', # 5분간격 (실시간x, 실시간데이터에 대해 2 ~ 2+5분 지연)
    start_date          = datetime(2026,1,1),
    catchup             = False,
    tags                = ['elk','opensearch', 'analysis', 'reader']
) as dag:
    # 3-1 오퍼레이터(검색)
    analysis_task = PythonOperator(
        task_id = 'analysis_task',
        python_callable= _analysis_task
    ) 
