#將資料放進Mysql資料庫，就不用需要資料就讀取自證交所
import numpy as np
import requests
import pandas as pd
import datetime
import json
import matplotlib.pyplot as pp
import time
import pymysql
MYSQL_HOST = 'localhost'
MYSQL_DB = 'stockprogram'
MYSQL_USER = 'root'
MYSQL_PASS = 'account119'

#   http://www.twse.com.tw/exchangeReport/FMTQIK?date=20190826

def connect_mysql():  #連線資料庫
    global connect, cursor
    connect = pymysql.connect(host = MYSQL_HOST, db = MYSQL_DB, user = MYSQL_USER, password = MYSQL_PASS,
            charset = 'utf8', use_unicode = True)
    cursor = connect.cursor()

def get_stock_history(date, retry = 5):
    quotes = []
    url = 'http://www.twse.com.tw/exchangeReport/FMTQIK?date=%s' % ( date)
    r = requests.get(url)
    data = r.json()
    return transform(data['data'])  #進行資料格式轉換

def transform_date(date):
        y, m, d = date.split('/')
        return str(int(y)+1911) + '/' + m  + '/' + d
    
def transform_data(data):
    data[0] = datetime.datetime.strptime(transform_date(data[0]), '%Y/%m/%d')
    data[1] = int(data[1].replace(',', ''))#把千進位的逗點去除
    data[2] = int(data[2].replace(',', ''))
    data[3] = int(data[3].replace(',', ''))
    data[4] = float(data[4].replace(',', ''))
    data[5] = float(data[5].replace(',', ''))
    return data

def transform(data):
    return [transform_data(d) for d in data]

def genYM(smonth, syear, emonth, eyear):  #產生從syear年smonth月到eyear年emonth月的所有年與月的tuple
    start = 12 * syear + smonth
    end = 12 * eyear + emonth
    for num in range(int(start), int(end) + 1):
        y, m = divmod(num, 12)
        yield y, m

def fetch_data(year: int, month: int):  #擷取從year-month開始到目前為止的所有交易日資料
    raw_data = []
    data = []
    today = datetime.datetime.today()
    for year, month in genYM(month, year, today.month, today.year): #產生year-month到今天的年與月份，用於查詢證交所股票資料
        if month < 10:
            date = str(year) + '0' + str(month) + '01'  #1到9月
        else:
            date = str(year) + str(month) + '01'   #10月
        data = get_stock_history(date)   #到證交所網站依照date抓取該月資料
        for item in data:  #取出該月的每一天資料
            selectsql = "select * from mtinf where date = '%s'"% (item[0])  #查詢是否已經在資料庫的SQL
            print(selectsql)
            cursor.execute(selectsql)  #執行查詢的SQL
            ret = cursor.fetchone()  #如果有取出第一筆資料
            if not ret:  #不在資料庫
                insertsql = "INSERT INTO mtinf (date, affareNum, affareAmount, affareAccount, taiex, upanddown) \
                VALUES ('%s', '%ld', '%ld', '%ld', '%f', '%f')" % (item[0], int(item[1]), int(item[2]), int(item[3]), float(item[4]), float(item[5]))   #插入資料庫的SQL
                print(insertsql)
                cursor.execute(insertsql) #插入資料庫
                connect.commit()    #插入時需要呼叫commit，才會修改資料庫
        time.sleep(10)  #延遲10秒，證交所會根據IP進行流量統計，流量過大會斷線

connect_mysql()
fetch_data(2019, 8)
print('完成寫入資料庫。')