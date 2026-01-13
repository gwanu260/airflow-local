from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging 
import random

def _branch_check(**kwargs):
    '''
    특정 조건에 따른 분기 담당
    '''
    # 랜덤하게 선택 (특정 목표가 없음)
    if random.choice([True, False]):
        logging.info('참 랜덤 선택, task_process 로 이동')
        return "process" # 이동하고 싶은 task의 task_id값을 표기
    else:
        logging.info('거짓 랜덤 선택, task_skip 로 이동')
        return "skip"    # 이동하고 싶은 task의 task_id값을 표기

    pass
def _process(**kwargs):
    logging.info('특정 업무 수행 성공')
    pass

with DAG(
    dag_id              = "04_basics_braching_v1",
    description         = "분기 처리, 조건에 따른 선택적 task 구동",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['brach', 'trigger_rule']
) as dag:
    # 오퍼레이터
    task_start   = EmptyOperator(
        task_id  = "start"
    ) # 시작
    task_branch  = BranchPythonOperator(
        task_id  = "branch_check",
        python_callable = _branch_check
    ) # 분기
    task_process = PythonOperator(
        task_id  = "process",
        python_callable = _process
    ) # 특정 업무
    task_skip    = EmptyOperator(
        task_id  = "skip"
    ) # 생략
    task_end     = EmptyOperator(
        task_id  = "end",        
        # NONE_FAILED_MIN_ONE_SUCCESS : 실패는 없고, 최소 1개는 성공했다
        # DAG는 모든 task는 성공해야 한다라는 기조로 작동하는 단위
        # 1개라도 중간에 실패하면 중단시킴 -> 전체 task 그대로 생략 해버림
        # 분기 진행 -> skip(분홍색 박스) 된 task가 반드시 발생 -> 모두 성공한다는 기조에 위배가 됨
        #  -> 특별한 설정이 없다면 모두 성공이라는 관점에 위배가됨
        # NONE_FAILED_MIN_ONE_SUCCESS
        # - 전체 공정에 실패는 없었다. 최소 1개 이상은 성공햇다 -> DAG은 신경 말고, 그대로 실행해라
        
        # 결론 브런치(분기기능) 사용시 필수값(기본값) trigger_rule를 적용함
        # 값은 NONE_FAILED_MIN_ONE_SUCCESS 세팅
        # 참고 : ALL_SUCCESS => skip을 실패로 간주함
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    ) # 종료

    # 의존성 (3가지 방향성)
    # task_branch 수행중 특정 조건값에 따라 task_process or task_skip 으로 선택 이동
    task_start >> task_branch
    task_branch >> task_process >> task_end
    task_branch >> task_skip >> task_end