# 1. 모듈 가져오기
from datetime import datetime, timedelta 
import logging
from airflow import DAG
# 아테나 대상으로 오퍼레이터 작업, 센서
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

# 2. 환경변수(상수값), s3 버킷등 경로
BUCKET_NAME = 'airflow-ai-en-4' # 사용자별 사용하는 버킷명
ATHENA_DB   = 'de_4' # Athena에 생성한 데이터베이스 이름
# 맨 마지막에는 /로 끝나야 함
ALAYSIS_KEY = f's3://{BUCKET_NAME}/athena-results/' # 분석 결과를 담을 s3상 경로(키) 

# 3. DAG 정의
with DAG(
    dag_id              = "10_aws_athena_query_v1",
    description         = "athena에 테이블 생성, 분석등 요청, 결과 저장",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily', # 수동실행 (트리거)
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['aws', 'athena','query']
) as dag:

    # 3-1. TASK 1 구현
    basic_table_create_task  = AthenaOperator(
        task_id = "basic_table_create_task",
        # 특정 저장소에서 있는 데이터 기반 테이블 생성, 구분자 , 이고, 1번라인은 헤더이므로 생략
        # 저장되는 형싟은 텍스트 지정
        query   = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS s3_exam_csv (
                id INT,
                name STRING,
                score INT,
                created_at STRING,
                result STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION 's3://{BUCKET_NAME}/data/'
            TBLPROPERTIES ("skip.header.line.count"="1");
        """,
        database        = ATHENA_DB,
        # 테이블 생성후 csv 읽어서 데이터룰 테이블에 주입하고 나온 결과물 저장할 위치
        # 원 데이터를 가진 테이블, 분석 결과를 가진 테이블 모두 같은 위치에 저장
        output_location = ALAYSIS_KEY,
        aws_conn_id     = 'aws_default'
    )
    
    # 3-2. TASK 2 구현
    # 분석 결과를 담고 있는 테이블을 DROP 처리 -> 먹통성 일관되게 유지하기 위한 조치사항
    # 해당 테이블은 데이터를 누적해서 가질수 없다 (ID가 없다) -> 신규 데이터 입력시 테이블 삭제 초기화
    report_table_drop_task   = AthenaOperator(
        task_id         = "report_table_drop_task",
        # 일일 보고서를 계속해서 유지하고 싶다면 삭제 task는 생략 가능 -> rdb나 s3 보관 추천
        query           = "drop table if exists daily_report_tbl",
        database        = ATHENA_DB,
        output_location = ALAYSIS_KEY,
        aws_conn_id     = 'aws_default'
    )
    
    # 3-3. TASK 3 구현
    report_table_create_task = AthenaOperator(
        task_id         = "report_table_create_task",
        # s3_exam_csv 테이블을 대상 => result 컬럼 기준 집계(group by result) => 
        # 결과 컬럼 (result, 개수(count), 평균점수(avg_score), 
        #           최소점수(min_score), 최고점수(max_score)를 결과로 담는 
        #           테이블 daily_report_tbl
        # 를 구성하는 쿼리를 작성하시오
        # PARQUET : 압축된 형태로 저장하는 형식중 하나(파케이) 속도는 빠르고, 비용이 저렴 형식
        query           = f"""
            CREATE TABLE daily_report_tbl
            WITH (
                format = 'PARQUET',
                external_location = 's3://{BUCKET_NAME}/report_data/'
            ) AS
            SELECT 
                result,
                COUNT(*) as count,
                AVG(score) as avg_score,
                MIN(score) as min_score,
                MAX(score) as max_score 
            FROM s3_exam_csv
            GROUP BY result
        """,
        database        = ATHENA_DB,
        output_location = ALAYSIS_KEY,
        aws_conn_id     = 'aws_default'
    )

    # 3-4. 의존성
    basic_table_create_task >> report_table_drop_task >> report_table_create_task