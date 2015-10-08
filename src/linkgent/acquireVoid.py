#!/usr/bin/env python
#coding=utf-8
import urllib2 ,sys,requests ,json,os,random
from bs4 import  BeautifulSoup
from bson.objectid import ObjectId
import pymongo 
from json import *
from gridfs import *
import StringIO 
from elasticsearch import Elasticsearch
import threading
import time
import platform
import codecs
import logging
from  urlcontent import UrlContent





global timers  
timers =0
file_dir_win ='D:\\audio\\'
file_dir_lin='/home/sam/audio/'

###################20150718##################################
def UsePlatform():
  sysstr = platform.system()
  if(sysstr =="Windows"):
    #print ("Call Windows tasks")  
    return file_dir_win
  elif(sysstr == "Linux"):
    return file_dir_lin
  else:
    return None



def getESCreate(logger):
    try:
        #es = Elasticsearch('es211.wodezoon.com',sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
        es = Elasticsearch('192.168.100.134',sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
        if es != None:
            return es
    except Exception ,e:
#         print'连接异常！',e 
        logger.info('ES Exception ')
        getESCreate(logger)  #重新连接
        
def connectDB(logger):  #####连接数据库
    try:
        client = pymongo.MongoClient("mongodb://192.168.100.179:27017")
        #client = pymongo.MongoClient("mongodb://mongodbrouter14.wodezoon.com:27017")
        db = client.radio   #创建 radio 数据库
        return db
    except Exception , e:
#         print 'Mongodb连接错误' ,e
        logger.info('Mongodb Exception' )
        connectDB(logger)
        
def selectVoidceclassify(logger):  ##查询分类
    headings = []
    try:
        db = connectDB(logger)
        for item in db.heading.find().sort('cid'):#顶级分类
            headings.append(item) 
        return headings
    except Exception ,e:
#         print e
        return headings

def selectshowInfo(cid,logger):
    showInfos = []
    try:
        db = connectDB(logger)
        for item in db.showInfo.find({'datacache':cid}):
            showInfos.append(item)
        return showInfos
    except Exception ,e:
#         print e
        return showInfos
    
def getPage(url,logger):          ####################获取页面
    try:
        page = UrlContent.getHtmlConnent(url)
        if page.getcode() == 200:            
            soup = BeautifulSoup(page.read())
            return soup
        else:
            logger.info("The server returns :",page.getcode())
            return None
    except Exception ,e:
#         print '服务器错误',url
        logger.info( 'server Exception'+url)
        return None


def elastics(audioInfo,es,logger):
    try:        
        headingName = audioInfo.get('cid')
        if headingName != None and ''!= headingName:
            if None != es:
                es.create('audio', headingName , audioInfo)
            else:
                es = getESCreate(logger)
                elastics(audioInfo,es,logger)
    except Exception ,e:
#         print'ES出错！',e
            logger.info('es Exception')
            logger.info(e)
            pass

def elastshowInfo(showIfo,es,logger):
    try:
        showmap = {}
#         print showIfo
        showmap['playcount']= showIfo['playcount']
        showmap['voiceName']= showIfo['voiceName']
        showmap['updatetime']= showIfo['updatetime']
        showmap['voicesort'] = showIfo['voicesort']         
        showmap['voicepages'] = showIfo['voicepages']                                         
        showmap['voiceurl'] =showIfo['voiceurl']
        showmap['datacache'] = showIfo['datacache']
        if None != es:
                es.create('radio', 'showInfo' , showmap)
        else:
            es = getESCreate(logger)
            elastshowInfo(showIfo,es,logger)
        pass
    except Exception,e:
#         print'ES出错！',e 
            logger.info('es elastshowInfo Exception')


def saveaudio(audios,db,es,logger,referer):
    
    if db == None:
        db = connectDB(logger)
    if es == None:
        es = getESCreate(logger)
    void_id = audios.get('void_id')
    url = 'http://www.ximalaya.com/tracks/'+void_id+'.json'
    logger.info(url)
    file_dir = UsePlatform()
    if file_dir ==None:  ####  默认为Windows
        file_dir = file_dir_win
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)
    dir = file_dir +void_id+'.txt' 
    if os.path.exists(dir):
        logger.info('File already exists !' +void_id)
        return None  
    page = UrlContent.getJSONConnent(url, referer)
    if page.getcode() != 200:
        return None
    page = page.read()
    #audios ={}
    compressedFile = StringIO.StringIO()     
    if page != None:
        try:
            date = json.loads(page)
            if date['play_path_64'] != None:
                duration = date['duration']   #音频时长
                audios['duration'] = duration
                play_count = date['play_count'] # 播放次数
                audios['play_count'] = play_count
                title = date['title']  #音频标题
                audios['title'] = title
                audios['void_id'] = void_id
                address = date['play_path_64']
                if None != address:
                    audios['address']=address
                    formatsrc = address.split('.')
                    if len(formatsrc)>0:
                        try:
                            outf =None
                            format =formatsrc[len(formatsrc)-1]
                            audios['format'] = format                           
#                            dir = file_dir +void_id+'.txt' 
#                             if os.path.exists(dir):
#                                  print '文件已经存在！',void_id
#                                 logger.info('File already exists !' +void_id)
#                                 return None
                            audio = UrlContent.getVoidConnent(address,referer)
                            audios['audios_dir'] = dir
                            outf = codecs.open(dir,'wb','utf-8') 
                            start = round(time.time())              
                            while True:
                                end = round(time.time())
                                if end - start > 700:
#                                     print '下载超时！',address 
                                    logger.info('Download the timeout！')
                                    return None
                                s = audio.read(1024*32)
                                if len(s) == 0:
                                    break 
                                compressedFile.write(s)                                                              
                            fs = GridFS(db,collection='audio')
                            gf = fs.put(compressedFile.getvalue(),filename=title+'.'+format,format=format,playcount=play_count,size=compressedFile.len)                           
                            audios['audio_id'] = str(gf)
                            audios['size'] = compressedFile.len 
                            outf.write('audio_id : '+ audios.get('audio_id')+ '  title : ' + audios.get('title'))
                            outf.flush()
                        except Exception,e:
#                             print '文件操作错误' ,e 
                            logger.info('File operations error')
                            audios['tag'] = '0'
                        finally:
                            if outf != None:
                                outf.close()   
                            compressedFile.close()
                            #exit()                           
                        audios['tag'] = '1'
                        #存到ES的方法
                        elastics(audios ,es,logger)                                        
                        result = db.audioInfo.insert(audios)   #存储音频信息                        
        except Exception,e:
#             print e
            logger.info(e,': Json ',url)
            return None
    return None

def getVoidInfo(url,cid,headingName,voiceName,db,es,logger):
    soup = getPage(url,logger) 
#     print url 
    logger.info(url)
    if None != soup:
        try:
            if soup.find('div', 'album_soundlist'):
                pagediv  = soup.find('div','album_soundlist')
                if pagediv != None:
                    lis = pagediv.find_all('li')
                    if lis !=None:
                        for li in lis:
                            void_id =''
                            if None != li['sound_id']:
                                void_id = li['sound_id']
                            lidiv = li.find('div','miniPlayer3')
                            if lidiv != None:
                                void_name =''
                                if None != lidiv.find('a','title'):
                                    void_name = lidiv.find('a','title').text
                                void_time ='' 
                                if None != lidiv.find('div','operate').span:
                                    void_time = lidiv.find('div','operate').span.text
                                playcount ='' 
                                if None != lidiv.find('span','sound_playcount'):   
                                    playcount = lidiv.find('span','sound_playcount').text                               
                            if void_id !='': 
                                audios={}   
                                audios['cid'] = cid
                                audios['void_time'] = void_time
                                audios['headingName'] = headingName
                                audios['voiceName'] = voiceName  
                                audios['void_id']  = void_id                             
                                #print void_id,void_name,void_time,playcount
#                                 print url,void_id
                                #threads(audios)
                                logger.info(url +'   ' + void_id)
                                audios = saveaudio(audios,db,es,logger,url)   
#                                 if None != audios:  
#                                     if '1' == audios.get('tag'):
#                                                                        
#                                         #存到ES的方法
#                                         elastics(audios)                                        
#                                         result = db.audioInfo.insert(audios)   #存储音频信息
#                                 else:
                                
                                continue                          
        except Exception, e:
#             print '解析出错～',e
            logger.info('Parse error')
            return None

def voidthreads():
    t1 = threading.Thread(target=crawlerVoid,args=())
    t1.setDaemon(False)
    t1.start()
 
def testsaver(logger):
    headings = selectVoidceclassify()
    es = getESCreate(logger)
    if len(headings) > 0 :
        for heading in headings:
            cid = heading.get('cid')
            headingName = heading.get('name')
            if cid != None:
                showInfos = selectshowInfo(cid)
                if len(showInfos) > 0:
                    for showInfo in showInfos:
                        elastshowInfo(showInfo,es,logger)

def crawlerVoid(logger):
    headings = selectVoidceclassify(logger)
    db = connectDB(logger) 
    es = getESCreate(logger)
    if len(headings) > 0 :
        for heading in headings:
            cid = heading.get('cid')
            headingName = heading.get('name')
            if cid != None:
                showInfos = selectshowInfo(cid,logger)
                if len(showInfos) > 0:
                    for showInfo in showInfos:
                        voiceurl = showInfo.get('voiceurl')
                        voicepages = showInfo.get('voicepages')
                        playcount = showInfo.get('playcount')
                        voiceName = showInfo.get('voiceName')
                        if playcount != None and playcount !='':
                            playcount = float(playcount)
                            if 1000000.0 >= playcount >50000.0:   #优先下载播放数大的
                                if voicepages != None and voicepages !='':
                                    for i in range(int(voicepages)):
                                        try:
                                            url = voiceurl + str(i+1)
                                            getVoidInfo(url,cid,headingName,voiceName,db,es,logger)
                                        except Exception, e:
#                                             print e
                                            logger.info(e)
                                            continue
    logger.info('crawlerVoid End!')
                                        
                                        
                                        

def voidMain():
    print 'start!!!'
    try:
        _pid  = os.fork()
        if _pid == 0:
            logger = logging.getLogger()
            file = logging.FileHandler('void.log')
            logger.addHandler(file)
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            file.setFormatter(formatter)
            logger.setLevel(logging.NOTSET)
            logger.info('进程ID:'+str(os.getpid()))
            crawlerVoid(logger)
        os.setsid()
    except Exception ,e:
       print e 
       os._exit(1)       
    print 'end!!'
voidMain()      


# class Ubuffered(object):
#     def __init__(self,stream):
#         self.stream = stream
#     def write(self,data): 
#         self.stream(data)
#         self.stream.fulsh()
#     def __getattr__(self,attr):
#         return getattr(self.stream,attr)         
#                 
#testsaver()                
#crawlerVoid() 
#
# if __name__ == '__main__':
#     crawlerVoid()
    
#selectVoidceclassify()
#selectshowInfo('1')    
#getVoidInfo('http://www.ximalaya.com/1000202/album/2667276?page=1')
#saveaudio('8279299')

