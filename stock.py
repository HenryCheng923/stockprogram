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

#   http://www.twse.com.tw/exchangeReport/STOCK_DAY?date=20180817&stockNo=2330

def connect_mysql():  #連線資料庫
    global connect, cursor
    connect = pymysql.connect(host = MYSQL_HOST, db = MYSQL_DB, user = MYSQL_USER, password = MYSQL_PASS,
            charset = 'utf8', use_unicode = True)
    cursor = connect.cursor()

def get_stock_history(date, stock_no, retry = 5):
    quotes = []
    print(date)
    url = 'http://www.twse.com.tw/exchangeReport/STOCK_DAY?date=%s&stockNo=%s' % ( date, stock_no)
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
    data[3] = float(data[3].replace(',', ''))
    data[4] = float(data[4].replace(',', ''))
    data[5] = float(data[5].replace(',', ''))
    data[6] = float(data[6].replace(',', ''))
    data[7] = float(0.0 if data[7].replace(',', '') == 'X0.00' else data[7].replace(',', ''))  # +/-/X表示漲/跌/不比價
    data[8] = int(data[8].replace(',', ''))
    return data

def transform(data):
    return [transform_data(d) for d in data]

def genYM(smonth, syear, emonth, eyear):  #產生從syear年smonth月到eyear年emonth月的所有年與月的tuple
    start = 12 * syear + smonth
    end = 12 * eyear + emonth
    for num in range(int(start), int(end) + 1):
        y, m = divmod(num, 12)
        yield y, m

def fetch_data(year: int, month: int, stockno):  #擷取從year-month開始到目前為止的所有交易日資料
    raw_data = []
    data = []
    today = datetime.datetime.today()
    for year, month in genYM(month, year, today.month, today.year): #產生year-month到今天的年與月份，用於查詢證交所股票資料
        if month < 10:
            date = str(year) + '0' + str(month) + '01'  #1到9月  '01是從每月1日開始'
        else:
            date = str(year) + str(month) + '01'   #10月   
        data = get_stock_history(date, stockno)   #到證交所網站依照date抓取該月股票編號為stockno的股價與成交量
        for item in data:  #取出該月的每一天編號為stockno的股票資料
            selectsql = "select * from twse where date = '%s' and stockno = '%s'"% (item[0], str(stockno))  #查詢是否已經在資料庫的SQL
            print(selectsql)
            cursor.execute(selectsql)  #執行查詢的SQL
            ret = cursor.fetchone()  #如果有取出第一筆資料
            if not ret:  #不在資料庫
                insertsql = "INSERT INTO twse (date, stockno, shares, amount, open, close, high, low, diff, turnover) \
                VALUES ('%s', '%s', '%ld', '%ld', '%f', '%f', '%f', '%f', '%f', '%d')" % (item[0], str(stockno),
                int(item[1]), int(item[2]), float(item[3]), float(item[4]), float(item[5]), float(item[6]), 
                float(item[7]), int(item[8]))   #插入資料庫的SQL
                print(insertsql)
                cursor.execute(insertsql) #插入資料庫
                connect.commit()    #插入時需要呼叫commit，才會修改資料庫
        time.sleep(10)  #延遲5秒，證交所會根據IP進行流量統計，流量過大會斷線

connect_mysql()
fetch_data(2018, 1, '2317')
print('完成寫入資料庫。')