'''
airflow 내부에서 관리하는 context 정보를 jinja를 이용하여 접근 사용 예시
DAG내에서 template 적용
'''
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging 

def _print(**kwargs):
    # airflow context는 함수 호출시 airflow가 injection 하여 전달 -> kwargs
    # ds, ds_nodash 테스크 수행시간
    logging.info(f'ds 출력 { kwargs["ds"] }')
    logging.info(f'ds_nodash 출력 { kwargs["ds_nodash"] }')
    pass

# 실습 DAG 기본 골격(형태) 구성 (02_xxx 참고)
with DAG(
    dag_id              = "03_basics_context_jinja_v1",
    description         = "Jinja 템플릿 적용, Context 접근, 매크로 사용",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=5)
    },
    # 초단위(맨앞), 년단위(맨뒤) 생략되었다면 => 아래 표기는 => 매일 오전 9시 0분에 실행
    schedule_interval   = '0 9 * * *', # cron 표기법
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['jinja','macro', 'context']
) as dag:
    # 1. 오퍼레이터 생성
    #    대부분 오퍼레이터 내부에는 template_field에 jinja가 세팅(허용)되어 있음
    #    {{ context의키값 }}
    t1 = BashOperator(
        task_id      ='template_used_bash',
        bash_command ='echo "테스트 수행시간은 {{ ds }}, {{ ds_nodash }} "'
    )
    # import airflow.macros => 하위 함수들은 private 하게 처리되어 있음 => __ 표기
    t2 = BashOperator(
        task_id      ='template_macro_used_bash',
        # ETL에서 지난주(어제 혹은 지난달등) 과거 데이터 조회시 필수 패턴중
        # airflow.macros 패키지를 의미 => import 없이 바로 사용 가능함
        bash_command ='echo "일주일전 수행시간은 {{ macros.ds_add(ds, -7) }} {{ macros.random() }}" '
    )
    t3 = PythonOperator(
        task_id      ='template_used_python',
        python_callable =_print
    )

    # 2. 의존성 관련 수행 순서 지정
    t1 >> t2 >> t3
    pass