'''
- 서비스
  매일 온라인 테스트(코테) 시험(특정 플랫폼)에 응시하는 학생들의 데이터가 쌓여서 일과후에 
  모두 특정 데이터베이스에 쌓임
- etl 처리 task가 해당 데이터베이스에 접속해서 18시(응시완료된시간)에 extract(추출)하여 
  s3 특정 공간에 저장 (csv 형태)
- 다른 task는 해당 csv를 대상으로 새로운 데이터를 기준으로 
  s3_exam_csv 테이블 삭제->생성->신규데이터 주입
- 다음날 00시 01분에 아래 스케줄 task 1 작동됨
'''
'''
일시적 분석 자료 -> Athena(서버리스)를 통해서 진행 -> 분석결과, 테이블내용 모두 s3에 있음
-> Athena를 통해 쿼리를 던지면 -> 가공되서 결과가 나옴

task 1 : 특정 s3 경로상에 데이터들 모두 삭제 -> 분석 데이터 삭제
         s3://버킷/athena/proccesed/pass_student/
         S3DeleteObjectsOperator 사용
         
task 2 : pass_student 정보를 가진 테이블 삭제 pass_student
task 3 : csv -> table (s3_exam_csv) 대상으로 90점 이상 학생들 정보를 모두 추출하여
         pass_student 테이블을 생성, 데이터 저장 (컬럼 : id, name, score, create_at)
task 4 : task3가 완료되었음을 센서를 붙여서 감시, 10초간격, 최대 대기시간 10분

task 1 > task 2 > task 3 > task 4
'''
'''
- 서비스 관리자는 아침 9시에 출근해서 -> 분석결과를 대시보드 조회 -> 의사결정
  - 문제 벨런스가 문제 잇나?, 합격자수가 너무 낮은데 등등... 회의 -> 문제 수준 조정
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta 
import logging
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 삭제용

# 2. 환경변수(상수값), s3 버킷등 경로
BUCKET_NAME   = 'airflow-ai-en-4'
ATHENA_DB     = 'de_4'
SRC_TABLE     = 's3_exam_csv'
TARGET_TABLE  = 'pass_student'

# s3 상에 저장할 위치는 작업중 설정
# csv, 메타데이터가 저장됨
S3_TARGET_LOC = f's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/' # 결과물 저장 위치
# Athena 쿼리 실행 로그 s3상 저장 위치
S3_QUERY_LOC  = f's3://{BUCKET_NAME}/athena/query_logs/'

# 3. DAG 정의
with DAG(
    dag_id              = "10_aws_athena_ctas_etl_v1",
    description         = "athena ctas 작업",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = None, # 수동실행 (트리거)
    start_date          = datetime(2026,1,1),
    catchup             = False,
    tags                = ['aws', 'athena','ctas']
) as dag:
    # DAG 다시 시작하면 관련되는 s3내 저장내된 내용, 테이블등을 모두 정리해 줌 -> 클리어
    #     매번 가동시 깨끗환 환경 상태 유지 -> 먹통성 유지 -> airflow의 스타일/철학 
    #     항상 동일하게 DAG(작업들이 ) 무결하게 진행됨
    # 1. 기존 데이터 정리(s3상에 저장된)
    t1 = S3DeleteObjectsOperator(
        task_id     = 'clean_s3_target',
        bucket      = BUCKET_NAME,      # 버킷 이름
        #keys       = [ .. ],           # 버킷 내 타겟들을 n개 지정 -> 풀경로 표기
        prefix      = f'athena/tbl/{TARGET_TABLE}/',
        aws_conn_id = 'aws_default',    # 접속 정보
    ) 
    # 2. 타겟 테이블 정리 -> 메타 데이터 정리
    t2 = AthenaOperator(
        task_id         = "table_drop_task",
        # 일일 보고서를 계속해서 유지하고 싶다면 삭제 task는 생략 가능 -> rdb나 s3 보관 추천
        query           = f"drop table if exists {ATHENA_DB}.{TARGET_TABLE}",
        database        = ATHENA_DB,
        output_location = S3_QUERY_LOC, # 로그 기록, 결과가 저장 위치
        aws_conn_id     = 'aws_default'
    )
    # 3. CTAS : csv -> table -> parguet 변환 및 저장
    #    테이블의 정보는 파케이,  압축 형태를 적용
    #    PARQUET/ORC => 열(컬럼)기반 형태
    #    압축 : GZIP, LZO, ZSTD, SNAPPY, ..
    query = f"""
        CREATE table {ATHENA_DB}.{TARGET_TABLE} 
        WITH (
            format              = 'PARQUET',
            parquet_compression = 'GZIP',
            external_location   = '{S3_TARGET_LOC}'
        )
        AS
        select id, name, score, created_at 
        from   {ATHENA_DB}.{SRC_TABLE} 
        where  score >= 90
        order by score desc
    """
    t3 = AthenaOperator(
        task_id         = "create_parguet_table",
        query           = query,
        database        = ATHENA_DB,
        output_location = S3_QUERY_LOC,
        aws_conn_id     = 'aws_default',
        do_xcom_push    = True
    )
    # 4. CTAS의 작업이 완료되었는지 감지(시)
    # 센서는 XCom (게시판으로 비유)에 "create_parguet_table" 이라는 이름으로 내용이 떳는지 체크
    t4 = AthenaSensor(
        task_id            = "sensor_task",
        query_execution_id = "{{ task_instance.xcom_pull(task_ids='create_parguet_table') }}",
        poke_interval      = 10,   # 10초 마다 감지
        timeout            = 600,  # 최대 대기 시간 - 10분
        aws_conn_id        = 'aws_default'
    )

    t1 >> t2 >> t3 >> t4