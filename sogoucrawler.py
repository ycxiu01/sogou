#! /opt/anaconda3/bin/python
# coding: utf-8

# In[3]:
import os
os.chdir('/home/yx2')

import logging
import logging.handlers  
import time
import requests,re,json
from bs4 import BeautifulSoup

tdate = time.strftime("%Y-%m-%d", time.localtime()) 
LOG_FILE = 'crawler_%s.log' %tdate

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 100*1024*1024, backupCount = 500) # 实例化handler   
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'  
  
formatter = logging.Formatter(fmt)   # 实例化formatter  
handler.setFormatter(formatter)      # 为handler添加formatter  
logger = logging.getLogger('')    # 获取名为''的logger  
logger.addHandler(handler)           # 为logger添加handler  
logger.setLevel(logging.INFO)  
  


class Crawler(object):
    def __init__(self):
        self.session = requests
    def get_main(self):
        headers={
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Accept-Encoding": "gzip,deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0(WindowsNT6.1;Win64;x64)AppleWebkit/537.36(KHTML,likeGecko)/60.0.3112.11",
            "Host": "zhishu.sogou.com",
            "Referer": "https://www.baidu.com/link?url=8dl8B5gTP_Eeep55BHEqbx0LcY7tcdrs-cE2ylG7iUd9RH137WkRi-8mI0Oaz",
            "If-None-Match": "W/\"2874-XLOW26ApYqVJ3TB/kov6cg\"",
            "Upgrade-Insecure-Requests": "1"
        }
        url="http://zhishu.sogou.com"
        response=self.session.get(url,headers=headers)
        #print(response.status_code)
        return(response.status_code)

    def get_index(self,keyword):
        url="http://zhishu.sogou.com/index/searchHeat"
        params={
                "kwdNamesStr":keyword,
                "dataType":"SEARCH_ALL",
                "queryType":"INPUT",
                "timePeriodType":"MONTH"
                }
        headers={
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Accept-Encoding": "gzip,deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0(WindowsNT6.1;Win64;x64)AppleWebkit/537.36(KHTML,likeGecko)/60.0.3112.11",
            "Host": "zhishu.sogou.com",
            "Referer": "http://zhishu.sogou.com",
            "Upgrade-Insecure-Requests": "1"
        }
        response=self.session.get(url,params=params,headers=headers)
        return(response)


# In[18]:

import datetime


# In[3]:

#crawler = Crawler()
#print(crawler)
#status = crawler.get_main()
#response = crawler.get_index(r'天然气')
#pattern = re.compile(r'root.SG.wholedata =\ (.*?)\;')
#result = pattern.findall(response.text)[0]
#result = json.loads(result)
#pvlist=[]
##print(result)
#for i in result["pvList"][0]:
#    pvlist.append({"date":i["date"],"pv":i["pv"]})
#data = {"pvlist":pvlist,"infoList":result["infoList"]}
#for value in data['pvlist'][0:10]:
#    print(value)


# In[14]:

from pandas.core.frame import DataFrame
import pymysql
import pandas.io.sql as sql
def DMFcon():
    con = pymysql.connect(host='10.254.11.240',
                port=3306,
                user='dmf',
                passwd='dmf2017',
                db='DMF20',
                charset='utf8')
    return(con)
gDMFcon = DMFcon()

def Researchcon():
    con = pymysql.connect(host='10.254.11.240',
                port=3306,
                user='research',
                passwd='research',
                db='research',
                charset='utf8')
    return(con)
gResearchcon=Researchcon()

def JYDBcon():
    con = pymysql.connect(host='121.40.50.179',
                port=3306,
                user='jydbuser',
                passwd='jydbdbuser',
                db='JYDB',
                charset='utf8')
    return(con)
gJYDBcon=JYDBcon()

def MySQLWithData(dbname,ssql):
    global gResearchcon
    global gDMFcon
    global gJYDBcon
    if dbname=='research':
        #print('research')
        con = gResearchcon
    if dbname=='dmf':
        #print("dmf")
        con = gDMFcon
    if dbname=='jydb':
        #print("dmf")
        con = gJYDBcon
    #print('ok')  
    try:
        data=sql.read_sql(ssql,con)
    except pymysql.Error as e:
        print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
        if dbname=='research':
            #print('research')
            gResearchcon=Researchcon()
            con = gResearchcon
        if dbname=='dmf':
            #print("dmf")
            gDMFcon=DMFcon()
            con = gDMFcon
        if dbname=='jydb':
            #print("dmf")
            gJYDBcon=JYDBcon()
            con = gJYDBcon()
        data=sql.read_sql(ssql,con)
    return(data)

def MySQLNoData(dbname,ssql):
    if dbname=='research':
        #print('research')
        con = pymysql.connect(host='10.254.11.240',
                port=3306,
                user='research',
                passwd='research',
                db='research',
                charset='utf8')
    if dbname=='dmf':
        #print("dmf")
        con = pymysql.connect(host='10.254.11.240',
                port=3306,
                user='dmf',
                passwd='dmf2017',
                db='DMF20',
                charset='utf8')
    # 获取游标
    cursor = con.cursor()
    try:
        cursor.execute(ssql) 
    except pymysql.Error as e:
        print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
        con.rollback() # 事务回滚
    else:
        con.commit() # 事务提交
    # 关闭连接
    cursor.close()
    con.close()


# In[39]:

#ssql= '''
#    CREATE TABLE IF NOT EXISTS HotTrend (
#    Date INT(11),
#    Keyword VARCHAR(20),
#    PV BIGINT(20) 
#    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
#    '''
#MySQLNoData('dmf',ssql)
#ssql='ALTER TABLE HotTrend ADD PRIMARY KEY(Date,Keyword);'
#MySQLNoData('dmf',ssql)
#ssql='ALTER TABLE HotTrend  ADD INDEX index_date(Date);'
#MySQLNoData('dmf',ssql)


# In[20]:

def Grab(kwd):
    #print(crawler)
    crawler = Crawler()
    status = crawler.get_main()
    if status==200:
        #starttime = datetime.datetime.now()
        response = crawler.get_index(kwd)
        pattern = re.compile(r'root.SG.wholedata =\ (.*?)\;')
        if pattern.findall(response.text):
            result = pattern.findall(response.text)[0]
            result = json.loads(result)
            logger.info(result)
            pvlist=[]
            #endtime =datetime.datetime.now()
            #print('crawler takes %d s' %(endtime - starttime).seconds)
            #print(result)
            for i in result["pvList"][0]:
                pvlist.append({"date":i["date"],"pv":i["pv"]})
            data = {"pvlist":pvlist,"infoList":result["infoList"]}
            #print(data)
            ssql="replace into HotTrend(Date,Keyword,PV) values"
            #starttime = datetime.datetime.now()
            for value in data['pvlist']:
                tdate = value['date']
                tpv = value['pv']
                ssql =  ssql+"(%d,'%s',%d)," %(tdate,kwd,tpv)
                #print(ssql)
            ssql=ssql[:(-1)]
            #endtime =datetime.datetime.now()
            #print('strconcat takes %d s' %(endtime - starttime).seconds)
            #print(ssql)
            MySQLNoData('dmf',ssql)
            print('ok with %s' %kwd )
            logger.info('ok with %s' %kwd)
            del(ssql)
        else:
            print('No data with %s' %kwd)
            logger.info('No data with %s' %kwd)
            #endtime =datetime.datetime.now()
            #print('crawler takes %d s' %(endtime - starttime).seconds)


# In[7]:

#Grab(r'天然气')


# In[12]:

ssql = 'select SecuCode,SecuAbbr from secumain WHERE SecuMarket IN (83,90) AND SecuCategory=1 AND ListedState=1'
secucode = MySQLWithData('jydb',ssql)
secucodelist = [secucode.iloc[x,0] for x in range(len(secucode))]
#print(secucode)


# In[73]:

#secucode.dtypes


# In[125]:

# ipython notebook --ip=127.0.0.1


# In[25]:

import re
import time
#for x in range(len(secucode)):
#    time.sleep(1)
#    print(x)
#    Grab(secucode.iloc[x,0])
#    abbr = re.sub(' ','',secucode.iloc[x,1])
#    abbr = re.sub('Ａ','A',abbr)
#    Grab(abbr)
#

# In[13]:

#re.sub(' ','',secucode.iloc[1,1])


# In[26]:

#abbr = re.sub(' ','',secucode.iloc[1,1])
#ssql = "select count(*) cnt from HotTrend where Keyword = '%s'" %abbr
#cnt = MySQLWithData('dmf',ssql)
#print(cnt)


# In[ ]:

#for i in range(441):
#    time.sleep(1)
#    abbr = re.sub(' ','',secucode.iloc[i,1])
#    ssql = "select count(*) cnt from HotTrend where Keyword = '%s'" %abbr
#    cnt = MySQLWithData('dmf',ssql)
#    if cnt.iloc[0,0]==0:
#        Grab(abbr)
#

# In[32]:

#ssql = "SELECT LTwoName FROM lc_conceptualsector"
#lname = MySQLWithData('jydb',ssql)
#useless = ['AB股','AH股','HS300','次新股','整体上市','S股','指数权重','ST概念','社保重仓','信托重仓','券商重仓','QFII重仓','基金重仓','保险重仓', '创业板','机构重仓','深成40','送转除权','预亏预减','预盈预增','融资融券','央视50','上证50','上证180','转融券','B股','创业成份','泽熙系','王亚伟系','景顺系','诺基亚','腾安价值','中证500','MSCI中国','创业板综','深证100R']


# In[49]:

conceptname =['阿里巴巴概念', '安防', '白酒', '白马股', '参股保险', '参股民营银行', '参股券商', '参股360', '参股新三板', '草甘膦', '超导', '超级品牌', '车联网', '充电桩', '创投', '大飞机', '大数据', '锂电池', '电子发票', '电子竞技', '电子商务', '电子信息', '迪士尼', '地下管网', '电力改革', '东盟自贸区', '二胎概念', '二维码识别', '风电', '分散染料', '氟化工', '福建自贸区', '高端装备', '高送转', '高铁', '高校', '工业4.0', '供应链金融', '股权转让', '广东自贸区', '光伏概念', '固废处理', '国产软件', '军工', '共享单车', '海工装备', '航运', '杭州亚运会', '核电', '互联网+', '互联网彩票', '互联网金融', '互联网医疗', '黄金', '沪港通概念', '互联网保险', '金融IC', '集成电路', '建筑节能', '家用电器', '节能环保', '节能照明', '金改', '京津冀一体化', '机器人概念', '基因测序', '健康中国', '军民融合', '举牌', '可燃冰', '跨境电商', '宽带中国', '蓝宝石', '冷链物流', '两桶油改革', '量子通信', '生态农业', '蚂蚁金服概念', '煤化工', '美丽中国', '民营医院', 'MSCI概念', '马云概念', '能源互联网', '农村电商', '农机', '农业现代化', 'O2O概念', 'OLED', 'P2P概念', '啤酒', '苹果概念', 'PM2.5', 'PPP概念', '汽车电子', '期货概念', '氢燃料电池', '禽流感', '区块链', '人工智能', '人脸识别', '融资融券', '乳业', '上海国资改革', '上海自贸区', '深港通', '生物医药', '石墨电极', '石墨烯', '食品安全', '首发新股', '手机游戏', '水利', '水泥', 'ST板块', '生物质能', '深圳国资改革', '钛白粉', '太阳能', '碳纤维', '特钢', '特高压', '腾讯概念', '特色小镇', '特斯拉', '天津自贸区', '天然气', '体育产业', '通用航空', '土地流转', '脱硫脱硝', '万达私有化', '网络游戏', '网约车', '王者荣耀', '尾气治理', '卫星导航', '文化传媒', '物联网', '物流电商平台', '无人机', '无人驾驶', '无人零售', '污水处理', '无线充电', '微信小程序', '雄安新区', '西安自贸区', '消费金融', '小金属', '细胞免疫治疗', '新材料概念', '新股与次新股', '新疆振兴', '新能源', '新能源汽车', '芯片概念', '网络安全', '稀缺资源', '稀土永磁', '虚拟现实', '养老概念', '央企国资改革', '页岩气', '一带一路', '移动互联网', '移动支付', '医疗改革', '医疗器械', '油品改革', '油品升级', '粤港澳概念', '云计算', '语音技术', '医药电商', '在线教育', '在线旅游', '债转股', '振兴东北', '智慧城市', '智能穿戴', '智能电网', '智能家居', '智能交通', '智能物流', '智能医疗', '智能音箱', '职业教育', '中韩自贸区', '中字头股票', '转融券标的', '猪肉', '证金持股', '摘帽', '装配式建筑', '足球概念', '租售同权', '自由贸易港', '3D打印', '4G5G']

# In[36]:

#lname


# In[41]:

#for i in range(len(lname)):
#    time.sleep(1)
#    abbr = re.sub(' ','',lname.iloc[i,0])
#    if abbr not in useless:
#        Grab(abbr)


# In[39]:

#len(useless)


# In[51]:

#for i in range(len(lname)):
#    time.sleep(1)
#    abbr = re.sub(' ','',lname[i])
#    #abbr = re.sub('概念','',abbr)
#    #abbr = re.sub('板块','',abbr)
#    if abbr not in useless:
#        Grab(abbr)


# In[16]:

#ssql = 'select distinct(Keyword) from HotTrend'
#kwlist = MySQLWithData('dmf',ssql)
def Updatedata(kwlist):
    for i in range(len(kwlist)):
        time.sleep(0.7)
        Grab(kwlist[i])
    if i%1000==0:
        time.sleep(20)

# In[21]:

#Grab('000001')
alllist = conceptname+secucodelist
print(alllist)
Updatedata(alllist)

ssql = 'select distinct(Keyword) from HotTrend where PV!=0 and Date in (select max(Date) from HotTrend)' 
kwmlist = MySQLWithData('dmf',ssql)
cnt = 0
while len(kwmlist)!=len(alllist) and cnt<=3:
    kwdiff = set(alllist).difference(set(kwmlist['Keyword']))
    Updatedata(list(kwdiff))
    ssql = 'select distinct(Keyword) from HotTrend where Date in (select max(Date) from HotTrend)' 
    kwmlist = MySQLWithData('dmf',ssql)
    cnt = cnt+1
    

logger.info('Done')  
# In[15]:

#ssql = 'select distinct(Keyword) from HotTrend'
#kwdf = MySQLWithData('dmf',ssql)
#kwlist = [kwdf.iloc[x,0] for x in range(len(kwdf))]
#'王者荣耀' in kwlist


