from google.oauth2 import service_account
from google.cloud import bigquery
from Crypto import Random
from Crypto.Cipher import AES
import base64
import pandas as pd



"""
7. DataLake(BQ) to DataWarehouse(BQ)
    => BQ to BQ는 조회해서 적재하면 됨
    스토리 : 90년대생 멤버들만 pw 암호화 해서 DW에 저장
"""
"""
create DW table query : 

create table dataset.memberDW(
    name string not null,
    mail string not null,
    password string not null,
    birth date
)
"""

def encrypt(pwd):
    password = pwd
    raw = pad(password).encode('utf-8')
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv) 
    password = base64.b64encode(iv + cipher.encrypt(raw))
    return password.decode('utf-8') # 장난하나 왜안되노🤛

def decrypt(enc):
    enc = base64.b64decode(enc)
    iv = enc[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return  unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')

def pad(s):
    return s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
def unpad(s):
    return s[:-ord(s[len(s)-1:])]

# API 요청에 필요한 구성
table_id = "hstest-316104.dataset.memberDW"
key_path = "C:\hstest-316104-7a2efb3e9c0e.json"
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

key = 'whycano__whycano'.encode('utf-8')
try :
    client = bigquery.Client(project = 'hstest-316104', credentials = credentials)

    query = """
        select name,
               mail,
               password,
               birth
        from dataset.memberDL
        -- where birth between '1991-01-01' and '1999-12-31'
    """
    result = client.query(query = query).to_dataframe()
    print("----------DL에서 조회 성공----------")

    # 블럭 사이즈 패딩 로직
    BS = 16


    # result.loc['password'] = aes(result['password'])

    # print(result)

    # result.replace("", aes(result['password']))
    # result.loc['password'] = aes(result['password'])
    
    list = []

    print("============기존 password============")
    print(result.password)

    # password에 암호화 알고리즘 적용
    for i in result.password:
        list.append(encrypt(i))

    result['password'] = list

    # for row in result:
        # password = row['password']
        # raw = pad(password).encode('utf-8')
        # iv = Random.new().read(AES.block_size)
        # cipher = AES.new(key, AES.MODE_CFB, iv) 
        # password = base64.b64encode(iv + cipher.encrypt(raw))
        # row["password"].replace(row["password"], password.decode('utf-8')) # 장난하나 왜안되노🤛

    """
        삽질의 흔적,,,,, 눈물,,,,,, 파이썬 몰라서 오늘도 나는 눈물을 흘린다,,,,,,,,,
    """

    # 결과물
    print("============결과물============")
    print(result)

    print("----------DW 적재 시작----------")

    job_config = bigquery.LoadJobConfig(
        schema=[
            # 순서가 필요가 없는게 빅쿼리에서 쿼리속도를 최척화하여 셔플함
            bigquery.SchemaField("name", "STRING", mode='required'),
            bigquery.SchemaField("mail", "STRING", mode='required'),
            bigquery.SchemaField("password", "STRING", mode='required'),
            bigquery.SchemaField("birth", "Date", mode='nullable'),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # 테이블 대체
    )

    load_job = client.load_table_from_dataframe(
        result, table_id, job_config = job_config  
    ) 

    load_job.result()

    destination_table = client.get_table(table_id)  
    
    print("--------BigQuery Connect && Load!--------")
    print("Loaded {} rows.".format(destination_table.num_rows)) 
    
    # dec_list = []
    # for i in list:
    #     dec_list.append(decrypt(i))

    # print("============디코딩 한 password============")
    # for i in dec_list:
    #     print(i)


except Exception as e : 
    print("----------Error----------")
    print(e)


"""
    헐 미친 진짜 완성❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗❗
"""