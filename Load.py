from os import replace
from google.cloud import bigquery
from google.oauth2 import service_account
"""
6. GCP(CSV in gcs to BigQuery)
https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#python
"""

"""
❗❗❗❗사전 작업 : Load.py 실행 전, gcp에 데이터셋과 테이블 만들기❗❗❗❗
데이터셋은 만들어 놨음 ^~^

create DL table query : 

create table dataset.memberDL(
    name string not null,
    mail string not null,
    password string not null,
    birth date
)

"""

table_id = "hstest-316104.dataset.memberDL"
key_path = "C:\hstest-316104-7a2efb3e9c0e.json"
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

try:
    conn = bigquery.Client(credentials = credentials)

    job_config = bigquery.LoadJobConfig(
        schema=[
            # 순서가 필요가 없는게 빅쿼리에서 쿼리속도를 최척화하여 셔플함
            bigquery.SchemaField("name", "STRING", mode='required'),
            bigquery.SchemaField("mail", "STRING", mode='required'),
            bigquery.SchemaField("password", "STRING", mode='required'),
            bigquery.SchemaField("birth", "Date", mode='nullable'),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # 테이블 대체
        skip_leading_rows = 1, # csv only (제일 첫 출 skip)
        source_format = bigquery.SourceFormat.CSV,
    )
    uri = "gs://oheong-test-bucket/result.csv"

    load_job = conn.load_table_from_uri(
        uri, table_id, job_config = job_config
    ) 

    load_job.result()

    destination_table = conn.get_table(table_id)  
    
    print("========BigQuery Connect && Load!========")
    
    # 왜 중복?ㅠ 해결~~ 걍 덮어버리기(WRITE_TRUNCATE),,
    print("Loaded {} rows.".format(destination_table.num_rows)) 
    

except Exception as e : 
    print("========BigQuery Connect Error!========")
    print(e)

