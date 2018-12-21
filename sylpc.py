# -*- coding: gbk -*-

import requests
import MySQLdb
import datetime
import time
from bs4 import BeautifulSoup

#由地址获取页面内容并封装为BeautifulSoup对象
def getSoup(url,type='get',code='utf-8',data={}):
    if type=='get':
        response = requests.get(url)
    elif type=='post':
        response = requests.post(url,data=data)
    else:
        return
    response.encoding = code
    return BeautifulSoup(response.text,'lxml')

#获取数据库链接
def getDb(host,user,password,dbName,charset='utf-8'):
    return MySQLdb.connect(host,user,password,dbName,charset=charset)

#获取昨天的日期，并转化为字符串
def getYesterday():
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days = -1) #减去一天
    return str(yesterday)

#采集行业市盈率并入库
def insertHySyl(date=getYesterday()):
    print '开始采集行业市盈率',date
    num = 0
    db = getDb('localhost','root','','stock','utf8')
    cursor = db.cursor()
    #url = 'http://www.cnindex.com.cn/syl/zxsyl/csrc_szzt.html'
    url = 'http://www.cnindex.com.cn/syl/%s/csrc_hsls.html' % date
    soup=getSoup(url,code='utf-8',type='get')
    table = soup.find('table',class_='table_01_box window0')
    if table is None:
        print '采集行业市盈率结束',date,'无数据'
        return 0
    trs = table.find_all('tr')
    for tr in trs:
        arr = []
        for td in tr.children:
            if td.name == 'td':
                arr.append(td.text.strip())
        #插入数据库
        sql = "insert into hysyl(type_id,type_name,gssl,jtsyl,dtsyl,in_date) values('%s','%s','%s','%s','%s','%s')" % (arr[0],arr[1],arr[2],arr[3],arr[5],date)
        cursor.execute(sql)
        db.commit()
        num=num+1
    print '采集行业市盈率结束',num,'条入库'
    return num
        
#采集个股市盈率并入库
def insertGgSyl(date=getYesterday()):
    print '开始采集个股市盈率',date
    db = getDb('localhost','root','','stock','utf8')
    cursor = db.cursor()
    url='http://www.cnindex.com.cn/stockPEs.do'
    json={
        'query.plate':u'深沪全市场',  #'深圳市场',
        'query.category':'008001',
        'query.industry':'A',
        'query.date':date,
        'pageNo':'1',
        'pageSize':'2000',
        'source':'2' 
    }
    num = 0 #记录入库条数
    for i in range(19):
        json['query.industry'] = chr(ord('A')+i)
        time.sleep(1)
        soup = getSoup(url,type='post',data=json)
        #解析soup
        table = soup.find('table')
        if table is None:
            print '采集个股市盈率结束',date,'无数据'
            return 0
        trs = table.find_all('tr')
        for i in range(1,len(trs)):
            tr = trs[i]
            arr = []
            for td in tr.children:
                if td.name == 'td':
                    arr.append(td.text.strip())
            #插入数据库
            if len(arr)>10 and arr[1]!='':
                sql = "insert into ggsyl(gp_id,gp_name,type_id0,type_name0,type_id,type_name,jtsyl,dtsyl,in_date) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s')"  \
                    % (arr[1],arr[2],arr[5],arr[6],arr[7],arr[8],arr[9],arr[10],date)
                #print arr[1],arr[2]
                cursor.execute(sql)
                db.commit()
                num = num+1
    print '采集个股市盈率结束',num,'条入库'
    return num

#采集个股行业划分
def insertGgHy(date=getYesterday()):
    return

#按日期获取市盈率
def getSyl(start=getYesterday(),end=getYesterday()):
    day=start
    dayObj=datetime.datetime.strptime(start,'%Y-%m-%d')
    if end>getYesterday():
        end = getYesterday()
    while day <= end:
        #获取行业和个股当日市盈率
        print 'start',day
        hyNum = insertHySyl(day)
        if hyNum>0:
            insertGgSyl(day)
        dayObj = dayObj + datetime.timedelta(days = 1) #增加一天
        day = dayObj.strftime('%Y-%m-%d')

#insertHySyl('2018-10-01')

#insertGgSyl('2018-10-01')
print "starting..."
start = raw_input("start date,eg:2018-01-01 >> ")
end =raw_input("end date,eg:2018-01-01 >> ")
getSyl(start,end)
raw_input("press enter to quit >> ")
