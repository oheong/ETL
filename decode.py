from google.oauth2 import service_account
from google.cloud import bigquery
from Crypto import Random
from Crypto.Cipher import AES
import base64
import pandas as pd



def decrypt(enc):
    enc = base64.b64decode(enc)
    iv = enc[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return  unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')

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
        from dataset.memberDW
    """
    result = client.query(query = query).to_dataframe()
    print("----------DL에서 조회 성공----------")

    # 블럭 사이즈 패딩 로직
    BS = 16

    list = []

    print("============암호화 된 password============")
    print(result)

    
    # password에 암호화 알고리즘 적용
    for i in result.password:
        list.append(decrypt(i))

    result['password'] = list

    print("============기존 password============")
    print(result)



except Exception as e : 
    print("----------Error----------")
    print(e)

