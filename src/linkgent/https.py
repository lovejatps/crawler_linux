#!/usr/bin/env python
#coding=utf-8

import urllib2 ,sys,os
from bs4 import  BeautifulSoup
from json import *
from bson.objectid import ObjectId
import pymongo 
import __main__
from __builtin__ import str
#from test.test_socket import try_address
from elasticsearch import Elasticsearch
import logging
import math
from _ast import Str


reload(sys)
sys.setdefaultencoding('utf8')


########## 顶级类-->细类-->节目[节目信息]-->音频信息及下载地址

def getESCreate(logger):
    try:
        es = Elasticsearch('192.168.100.134',sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
        #es = Elasticsearch('es211.wodezoon.com',sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
        if es != None:
            logger.info('ES OK')
            return es
    except Exception ,e:
        #print'连接异常！',e 
        logger.info('ES Exception！')
        getESCreate(logger)  #重新连接

def connectDB(logger):
    try:
        #client = pymongo.MongoClient("mongodb://mongodbrouter14.wodezoon.com:27017")
        client = pymongo.MongoClient("mongodb://192.168.100.179:27017")
        db = client.radio   #创建 radio 数据库
        logger.info('Mongodb OK')
        return db
    except Exception, e:
        #print 'Mongodb连接错误',e
        logger.info('Mongodb Exception')
        connectDB(logger)
        
def elastics(audioInfo,es,logger):
    try:
        if None != es:
            audiomap ={}
            audiomap['url']=audioInfo['url']
            audiomap['name']=audioInfo['name']
            audiomap['cid']=audioInfo['cid']
            if es !=None:
                es.create('radio', 'sort' , audiomap)
            else:
                es =  getESCreate(logger)
                elastics(audioInfo, es,logger)
    except Exception ,e:
     #   print'ES出错！',e 
        logger.info('ES index audion Exception！')

def elaseicsshowInfo(showInfo,es,logger):
    try:
        es =  getESCreate(logger)
        if None != es:
            es.create('radio', 'showInfo', showInfo)
        else:
             es =  getESCreate(logger)
             elaseicsshowInfo(showInfo,es,logger)
    except Exception ,e:
        #print 'ES出错！',e
        logger.info('ES index showInfo Exception！')

###############存储分类
def serversort(jsondata,types,db,es,logger):
    if types == 0:
        result = db.heading.insert(jsondata)   #顶级分类
        elastics(jsondata,es,logger)
    else:
        result = db.subclass.insert(jsondata)  #细类



def getPage(url,logger):          ####################获取页面
    try:
        page = urllib2.urlopen(url).read()
        soup = BeautifulSoup(page)
        return soup
    except Exception, e:
       # print '服务器错误' ,e
        logger.info('server Exception' )
        return None


def getclassify(url,logger):           ####################获取分类
    soup = getPage(url,logger)
    if None != soup:
        li = soup.find('ul','sort_list')
        heading=[]
        sunnames = []
        a = 0
        for i in li.find_all('li'):
           cid =  i['cid']
           url =  i.a['href']
           name = i.a.text
           id = ObjectId()
           limap ={'cid':cid,'url':url,'name':name,"_id":str(id)}
           #print limap 
           logger.info(limap)
           heading.append(limap)
        

###################子分类####################              
        divli = li.find('div','tag_wrap')
        for d in divli.find_all('div'):   
            datacache = d['data-cache']
            for subs in d.find_all('a'):
                suburl = subs['href']
                sunName = subs.text
                sunmaps = {'suburl':suburl,'sunName':sunName,'datacache':datacache,"_id":ObjectId()}
                sunnames.append(sunmaps)
        return heading, sunnames
    else:
        return heading,sunnames

def getpagenumber(soup,logger):  #####################获取总页数
    try:
        page = soup.find('div','pagingBar_wrapper')
        if None == page:
            return '1'
        a = 0
        if None != page.find_all('text','下一页'):
            pages = []
            for i in page.find_all('a'):
                p = i.text
                a = a + 1
                pages.append(p)
                if '下一页' == p:
                    a = a -1
                    break;
            return str(pages[a-1])
        else:
            return '1'
    except Exception, e:
       # print '获取总页数错误',e
        logger.info('pages Exception')


def getclassifyList(url,logger):  #####################获取节目列表的URL【每一页的URL】
    soup = getPage(url,logger)
    if None != soup:
        pages = getpagenumber(soup,logger)
        urls = []
        for page in range(int(pages)):
            #print url + str(page+1)
            logger.info(url + str(page+1))
            urls.append(url + str(page+1))
        if len(urls) < 1:
            urls.append(url)
        return urls
    else:
        return urls


def getvoiceURL(url,logger): #####################获取每一页的各个节目URL
    urls =[]
    soup = getPage(url,logger)
    if None != soup:
        divs = soup.find('div','discoverAlbum_wrapper')
        if None != divs:
            for li in divs.find_all('div','albumfaceOutter'):
                urls.append(li.a['href'])
            return urls
        else:
            return urls
    else:
        return urls

def getvoiceInfo(url,datacache,logger):#####################获取一个节目信息与下载地址
    voiceInfomap = {}
   # print url
    soup = getPage(url,logger)
    if None != soup:
        plays = soup.find('div','detailContent_playcountDetail')
        if None != plays:
            plays= plays.find('span').text
        if None != plays:
            if plays.find('万') > 0:
               plays = float(plays.rstrip('万')) * 10000    
        else:
             plays = 0 
        try:    
            voiceInfomap['playcount'] = str(plays)                ##########播放数量
            if None!= soup.find('div','detailContent_title'):
                voiceName =soup.find('div','detailContent_title').text
            else:
                voiceName =''
            voiceInfomap['voiceName'] = voiceName                           ##########节目名称
            if None !=soup.find('div','detailContent_category'):
                if None != soup.find('div','detailContent_category').find('span','mgr-5'):
                    updatetime = soup.find('div','detailContent_category').find('span','mgr-5').text
                else:
                    updatetime =''
            else:
                updatetime =''
            voiceInfomap['updatetime'] = updatetime  ##########最后更新时间
            if None != soup.find('div','detailContent_category'):
                if None != soup.find('div','detailContent_category').find('a'):
                    voicesort = soup.find('div','detailContent_category').find('a').text.lstrip('【').rstrip('】') 
                else:
                    voicesort =''
            else:
                voicesort=''           
            voiceInfomap['voicesort'] =voicesort              ##########分类名称
            voiceInfomap['voicepages'] = getpagenumber(soup,logger)                                                  ##########节目的音频总页数
            voiceInfomap['voiceurl'] = url +'?page=' 
            voiceInfomap['datacache'] = datacache
            db = connectDB(logger)
            es = getESCreate(logger)
            elaseicsshowInfo(voiceInfomap,es,logger) 
            voiceInfomap['_id'] = ObjectId()
            result = db.showInfo.insert(voiceInfomap)   #节目信息存储
            
            if result == None:
                result = db.showInfo.insert(voiceInfomap)   #重e新存储节目信息
            
            return result
        except Exception, e :
          #  print 'HTML 解析错误~',e
            logger.info('HTML Exception~')
            return None
    else:
        return None



#classifyInfo()

def classifyInfo(logger):  ###########下载分类【顶级分类与细类】
    headings ,sublist=getclassify('http://album.ximalaya.com/',logger)
    db = connectDB(logger)
    es =  getESCreate(logger)
    if len(headings) > 0:
        for heading in headings:
            print 'JSON:',JSONEncoder().encode(heading)
            serversort(heading, 0,db,es,logger)
    if len(sublist) > 0:
        for subclass in sublist:
            serversort(subclass, 1,db,es,logger)
    logger.info('classifyInfo End')

def selectVoidceclassify(logger):  ##查询分类
    headings = []
    subclasss= []
    db = connectDB(logger)
    for item in db.heading.find():#顶级分类
        headings.append(item) 
    for item in db.subclass.find():#细分类
        subclasss.append(item) 
    return headings,subclasss

def saveVoidceclassifyES(logger):  # 存放将分类在ES创建索引
    heads,sublist=selectVoidceclassify(logger)
    es =  getESCreate(logger)
    for x in heads:
        elastics(x,es,logger)
        


def servershowInfo(logger):
    heads,sublist=selectVoidceclassify(logger)
   # print len(sublist)
    try:
        for x in sublist:
            suburl = x.get('suburl',None)
            datacache = x.get('datacache',-1)
            if None != suburl :
               urls = getclassifyList(suburl,logger)#获取节目列表的URL【每一页的URL】
               if len(urls) > 0:
                    for u in urls:
                        urlps = getvoiceURL(u,logger)#获取每一页的各个节目URL
                        if len(urlps) > 0:
                            for urlp in urlps:
                                getvoiceInfo(urlp,datacache,logger)
    except Exception, e:
        logger.info('Exception')      
	logger.info('servershowInfo End')
#saveVoidceclassifyES()
  
def voidMain():
    print 'start!!!'
    try:
       # _pid  = os.fork()        
        #if _pid == 0:
        logger = logging.getLogger()
        file = logging.FileHandler('showInfo.log')
        logger.addHandler(file)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        file.setFormatter(formatter)
        logger.setLevel(logging.NOTSET)
        logger.info('ID:'+str(os.getpid()))
        classifyInfo(logger)
        #servershowInfo(logger)
        os.setsid()
    except Exception ,e:
       print e 
       os._exit(1)
       
    print 'end!!'
    
    
voidMain()      
  
# if __name__ == '__main__':
#     servershowInfo()
# #    
