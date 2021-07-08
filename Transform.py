import pandas as pd
import Connect
"""
4. DB to CSV
"""

# DB에 연결 후 가져온 값
list = Connect.rows

for i in list:
    print(i)
print("=====================================")
print(str(len(list))+"개의 데이터 조회 완료")
print("=====================================")

# csv 파일 헤더 행 추가
col_name = ['이름', '메일', '비밀번호', '생년월일']

# 리스트 데이터프레임에 저장
df = pd.DataFrame(list, columns = col_name)

# 파일 저장 이름, 인덱스 없음, 인코딩
df.to_csv('test_20210624.csv', index = False, encoding = "utf-8-sig") # 그냥 utf-8하면 한글 오류남 다른게뭐지?
print("&&&&&&&&csv 생성 완료&&&&&&&&")
