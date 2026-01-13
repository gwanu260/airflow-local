# 기본 골격, bash 오퍼레이터 적용
# airflow 상에서 dag가 어떻게 작동하는지 기초
# timedelta : 시간의 차이를 계산하는 함수
from datetime import datetime, timedelta 
# DAG
from airflow import DAG
# 오퍼레이터 2.x
from airflow.operators.bash import BashOperator
# 오퍼레이터 3.x
#from airflow.providers.standard.operators.bash import BashOperator

# 1. 기본 인자 구성 -> task에 적용될 매개변수
#    dag 소유주(작성자, 관리자), 
#    과거 데이터 누락분에 대한 소급 실행 여부, 
#    작업 실패(빈번하게(I/o일 확률이 높다) 발생될수 있음)가 발생했을때 재시도 여부->1회만등등 설정
#    실패후 다시 시도할때 텀(간격) 설정. 몇분후 다시 시도
default_args = {
    'owner'          :'de_1team_manager', # DAG 주인
    'depends_on_past': False,             # 과거 데이터 소급 처리 금지
    'retries'        : 1,                 # 작업 실패시 재시도는 1회 자동 진행
    'retry_delay'    : timedelta(minutes=5) # 실패 시 5분 후에 재시도
    # 시나리오 => 작업 성공 => 완료
    # 시나리오 => 작업 실패 => 5분후 => 1회 재시도 => 성공 => 완료
    # 시나리오 => 작업 실패 => 5분후 => 1회 재시도 => 실패 => 해당 시점의 작업 포기(소급 방지)
}

# 2. DAG 정의
'''
with DAG() as dag:
    # 오퍼레이터 등등.. 기술
    # 작업 순서 (의존성 고려) 지정
'''
with DAG(
    dag_id          = "01_basics_bash_v1", # 고유한 값, DAG를 상호 구분하는 용도
    description     = "DE를 위한 ETL 작업의 핵심 패키지 airflow 기본 연습용 DAG", # DAG 설명
    default_args    = default_args, # DAG의 기본 인자값
    schedule_interval = '@daily',    # 하루에 한번. 매일 00시 00분(자정)에 실행, cron 표현가능
    start_date      = datetime(2025,1,1), # 과거날짜로(샘플) 설정. 특정시점 세팅해도 됨 -> 소급 수행 가능성 존재함. 즉시 수행되는 장점
                                          # 과거 설정의 장점은 당장 지금부터 시점이 되면 수행됨
    catchup         = False,        # 과거에 대한 소급 처리 실행 방지
    # 만약 False 아니면 1년치(365일) + 현재까지(8일) => 365+8 회 수행이됨(소급 처리 진행됨)
    # 개발, 신규 서비스 런칭 => 소급처리 방지하여 구성
    tags            = ['bash','basic'] # dAG 많으면 찾기 힘듬 => 검색용
) as dag:
    # 오퍼레이터 -> 작업자
    # Task 정의 -> 내용 자체에는 목적이 현재는 없음 (수행되는지만 관심을 가짐)
    # task 작동 내용을 로그를 통해서 확인 가능
    t1 = BashOperator(
        # 영문자, 숫자, 하이픈, 마침표, 밑줄만으로 구성되어야 합니다.
        task_id      ='date-print', # aiwflow의 지휘를 통해서 dag 구동시 실제 할일을 정의한 task 구분값
        bash_command ='date'      # 리눅스의 date 명령
    )
    t2 = BashOperator(
        task_id      ='sleep',
        bash_command ='sleep 5'   # 5초 대기
    )
    t3 = BashOperator(
        task_id      ='print_echo',
        bash_command ='echo "반갑습니다. Airflow start 123!@#"'
    )

    # 의존성
    # t1 실행 -> t2 실행 -> t3 실행
    # t1 실행이 성공해야만 t2 실행함, t2 실행이 성공해야만, t3 실행
    # 대시보드에서 Graph를 통해서 노드의 연결방향이 표현됨
    t1 >> t2 >> t3