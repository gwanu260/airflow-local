'''
스마트팩토리에 설치된 센서에서 발생된 로그를 aws opensearch 서비스에 전송하는 DAG
주기는 1 OR 5분 간격으로 스케줄링
- 파이썬 -> 1분 주기 -> opensearch
- 파이썬 -> 1분 주기(컨셉부여 or 발생즉시:실시간) -> s3(원본) / Fluent Bit(검색엔진으로 보내는)
        -> opensearch -> 검색 -> s3 저장 -> athena 분석 -> 대시보드 레포팅
        
- 설치 패키지
- opensearch-py : 파이썬 레벨에서 직접 접속하여 검색엔진 접속 및 업무
- apache-airflow-providers-opensearch : airflow 기반에서 검색엔진 접속 및 업무

'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import random
import time
import pendulum
# Generic 타입의 커넥션 정보 획득
from airflow.hooks.base import BaseHook
# 환경변수에서 획득
from airflow.models import Variable

# 2. 설정 정보 -> OpenSearch
conn = BaseHook.get_connection('opensearch_default')
host = Variable.get("OS_HOST")#, deserialize_json=True) # 뒤옵션을 풀면 JSON 형태로 출력됨(역직렬화)

# 도메인 엔드 포인트(IPV4)
HOST        = conn.host
AUTH        = (conn.login, conn.password) # master 계정 정보 (숨김), .env나 airflow에 설정값등
# A 공장 45구역에 모든 센서 데이터 인덱스 정보 -> 커스텀 설정
INDEX_NAME  = 'a-factory-45x-sensor-v1' # 해당 데이터를 검색할 수 있는 분류할 수 있는 인덱스 정보


# 파이썬 레벨(airflow 없이 사용할 수 있다)
def _send_log_task(**kwargs):
    logging.info( '-'*20 )
    logging.info( host )
    logging.info( '-'*20 )
    logging.info( conn.host )
    logging.info( '-'*20 )
    
    # 1. 오픈서치 모듈 가져오기 -> 에러 발생시 확실한 위치 확인차원
    # 로컬(가상환경)에서 직접 접근시
    from opensearchpy import OpenSearch
    # 2. 오픈서치 클라이언트 연결 -> aws 상에 오픈서치 관련 도메인 구성해야함
    client = OpenSearch(
        hosts               = [ { "host":HOST, "port":443 } ],
        http_compress       = True,
        http_auth           = AUTH,
        use_ssl             = True,
        verify_certs        = True,
        ssl_assert_hostname = False,
        ssl_show_warn       = False
    )
    # 3. 인덱스 확인 (절차)
    # 해당 로그를 구분할 수 있는 표식 -> 인덱싱 수행 가능함
    # opensearch 상에 등록된 인덱스 값이 존재하는 체크 -> 없으면 생성 -> 1회성
    if not client.indices.exists(index=INDEX_NAME):
        # 없으면 생성
        client.indices.create(index=INDEX_NAME)
        logging.info(f'인덱스 생성 {INDEX_NAME}')
        
    logging.info('가상 센서 데이터 전송 (Batch 작업:특정 반복 주기로 진행, 실시간 x, 지연 존재)')
    
    # 4. 장비 고유값 정의( n개의 센서의 고유값 정의(문자열) )
    oven_ids = ['OVEN_001','OVEN_002','OVEN_003']
    
    # 5. 로그 발생
    # 장비별로 30회 로그를 임의 발생 -> 전송 (회차별 장비 3개의 로그값 전송)
    MAX_LOOP = 30
    for i in range(MAX_LOOP):
        for oven in oven_ids:
            # 데이터 랜덤 생성
            temp = random.uniform(100, 200) # 오븐 온도 생성
            # 임의 변조
            if random.random() > 0.95: #5% 확률로 변조
                temp += random.uniform(30, 50)
            # 데이터 구성
            doc = {
                'timestamp' :  pendulum.now(tz="Asia/Seoul"), # 로그 발생 시간
                'oven_id'   : oven,            # 센서 장비 id
                'temperature' : round(temp,2), # 온도(소수점 2자리까지)
                'vibration' : round( random.uniform(0, 1.5), 2 ),  # 진동 레벨 임의 구성
                'status'    : 'DANGER' if temp > 230 else 'NORMAL' # 센서 감지상 위험/평시
            }
            # 시나리오
            # 특정 기간동안 특정센서에서 DANGER 가 지속적으로 검색되면 => 이상신호로 볼 수 있음
            # 전송
            client.index(
                index = INDEX_NAME,
                body  = doc,
                refresh = True
            )
        # 전송률 로깅 -> 특정 텀 단위로 진행 -> 5번에 한번식 로깅
        if i % 5:
            logging.info(f'{i+1}번차 로그 전송 성공')
        
        # 시간 임의 지연 -> 2초
        time.sleep(2)
        
    
    # 6. 로그 전송
    # 7. 배치 작업 완료
    logging.info('n차 로그 발생 완료')
    pass

with DAG(
    dag_id              = "11_ELK_log_generator_v1",
    description         = "파이썬 기반으로 가상으로 로그 발생",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },
    schedule_interval   = '*/2 * * * *', # 5분간격
    start_date          = datetime(2026,1,1),
    catchup             = False,
    tags                = ['elk','opensearch', 'sensor', '스마트팩토리']
) as dag:
    send_log_task = PythonOperator(
        task_id         = "send_log_task",
        python_callable = _send_log_task
    )