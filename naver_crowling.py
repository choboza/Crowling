from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymysql

# 전체 변수
conn = pymysql.connect(host='localhost', user='root', password='1111',
                             db = 'investor', charset ='utf8')
cur = conn.cursor()

# krx 종목코드 크롤링
def krx_code():
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'

    df = pd.read_html(url, header=0)[0]
    df = df[['회사명', '종목코드']]
    df['종목코드'] = df['종목코드'].map('{:06d}'.format)
    print(df)
    return df


# krx db에 넣기 
def insert_krx(df):
    # 메인코드 - 설명
    conn = pymysql.connect(host='localhost', user='root', password='1111',
                         db = 'investor', charset ='utf8')
    cur = conn.cursor()

    # 메인코드
    cur.execute('Create table if not exists company_info (id varchar(20), id_code varchar(10), primary key(id_code))')

    for i in range(len(df)):
        sql = "replace INTO company_info (id, id_code) VALUES (%s, %s)"
        val = (df['회사명'][i], df['종목코드'][i])
        cur.execute(sql, val)   

    conn.commit()
    conn.close()


# db에서 조목코드 뽑아오기
def select_krx():
    conn = pymysql.connect(host='localhost', user='root', password='1111',
                        db = 'investor', charset ='utf8')
    cur = conn.cursor()
    cur.execute('select id_code from company_info')
    com_list = []
    while(True):
        row = cur.fetchone()
        if row == None:
            break
        com_list.append(row)
    com_list

    return com_list


# 주식가격 테이블 만들기

def stock_table():
    conn = pymysql.connect(host='localhost', user='root', password='1111',
                             db = 'investor', charset ='utf8')
    cur = conn.cursor()

    sql = """
                CREATE TABLE IF NOT EXISTS daily_price (
                    code VARCHAR(20),
                    date DATE,
                    open BIGINT(20),
                    high BIGINT(20),
                    low BIGINT(20),
                    close BIGINT(20),
                    diff BIGINT(20),
                    volume BIGINT(20),
                    PRIMARY KEY (code, date))
                """
    cur.execute(sql)
    conn.commit()
    
    return print('주식 테이블 만듬')

# page 정하기

def page_select(com_list, page):
    url = 'http://finance.naver.com/item/sise_day.nhn?code=%s'%com_list[i][0]
    res = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(res.content, 'html.parser')
    data = soup.select_one('td.pgRR')
    s = data.a['href'].split('=')
    last_page = int(s[-1])
    my_page = min(last_page, int(page))
    print(my_page)
    return (my_page)


# 주식데이터 인서트하기

def insert_db(data):
    for idx in range(len(data)):
        code = com_list[i][0]
        date = data.date[idx]
        open = data.open[idx]
        heigh = data.heigh[idx]
        low = data.low[idx]
        close = data.close[idx]
        diff = data['diff'][idx]
        volume = data.volume[idx]
        sql = f"replace INTO daily_price (code, date, open, high, low, close, diff, volume) VALUES ('{code}','{date}','{open}','{heigh}','{low}','{close}','{diff}','{volume}')"
        cur.execute(sql) 

# 주식데이터 정제하기

def make_db(j):
    url = 'http://finance.naver.com/item/sise_day.nhn?code=%s&page=%d'%(com_list[i][0], j+1)
    response_page = requests.get(url, headers={'User-agent': 'Mozilla/5.0'}).text
    data = pd.read_html(response_page)[0]
    data = data.dropna()
    data = data.reset_index(drop=True)
    data = data.rename(columns = {'날짜': 'date','종가':'close','시가':'open','고가':'heigh','저가':'low','거래량':'volume','전일비':'diff'})
    return data



#### 총합본 조심해서 건들이시오

krx_data = krx_code()
insert_krx(krx_data)
com_list = select_krx()
stock_table()

for i in range(len(com_list)):                                                      
    my_page = page_select(com_list, 2)
        
    for j in range(int(my_page)):
        data  = make_db(j)
        insert_db(data)
    
    conn.commit()
    
conn.close()
