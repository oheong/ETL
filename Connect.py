import psycopg2

"""
1. 대상 DB접속
2. postgresql 원천 데이터 조회 
3. postgresql 원천 데이터 추출
"""

try:
    print("*************DB Connected*************")
    # DB 연동
    db = psycopg2.connect(host='localhost',
                        dbname='ABC-test',
                        user='postgres',
                        password='oheong',
                        port=5432)

    # 명령 처리할때 사용
    cursor = db.cursor()

    # 실행 할 쿼리문 (원천 데이터)
    sql_job : str = rf'''
        select 
               name,
               mail,
               password,
               to_char(birth,'YYYY-MM-DD')
        from member
        where 1=1
        order by no;
    '''

    # Extract
    # 쿼리 실행
    cursor.execute(sql_job)
    rows=cursor.fetchall()

    # 출력 (잘 나옴)
    # for i in rows:
    #     print(i)

except psycopg2.DatabaseError as db_err:
    print("==========Not Connected!==========")
    print(db_err)
