#!/usr/bin/env python
#coding=utf-8
import urllib2 ,random

class UrlContent(object):
    my_header=[
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.69 Safari/537.36 QQBrowser/9.1.3471.400",
        "Mozilla/5.0 (X11; Ubuntu; Linux i686;rv:10.0) Gecko/20100101 Firefox/10.0",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36",
        "Mozilla/5.0 (Windows;U;Windows NT 6.1;en-US;rv:1.9.1.6)Gecko/20100101 Firefox/27.0",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.93 Safari/537.36"
    ]
    @staticmethod
    def getHtmlConnent(url):
        print url 
        req = urllib2.Request(url)
        useragent = random.choice(UrlContent.my_header)
        req.add_header("User-Agent",useragent)
        req.add_header("Host","www.ximalaya.com")
        req.add_header("Referer","http://album.ximalaya.com")
        req.add_header("GET",url)
        connent = urllib2.urlopen(req)
        return  connent
    @staticmethod
    def getJSONConnent(url,referer):
        req = urllib2.Request(url)
        useragent = random.choice(UrlContent.my_header)
        req.add_header("User-Agent",useragent)
        req.add_header("Accept",'application/json, text/javascript, */*; q=0.01')
        req.add_header("Content-Type:","application/x-www-form-urlencoded")
        req.add_header("Host","www.ximalaya.com")
        req.add_header("Referer",referer)
        req.add_header("X-Requested-With","XMLHttpRequest")
        req.add_header("GET",url)
        connent = urllib2.urlopen(req)
        return  connent

    @staticmethod
    def getVoidConnent(url,referer):
        req = urllib2.Request(url)
        useragent = random.choice(UrlContent.my_header)
        req.add_header("User-Agent",useragent)
        req.add_header("Accept",'*/*')
        req.add_header("Accept-Encoding",'identity;q=1, *;q=0')
        req.add_header("Connection:","keep-alive")
        req.add_header("Host","audio.xmcdn.com")
        req.add_header("Referer",referer)
        req.add_header("Range","bytes=0-")
        req.add_header("GET",url)
        connent = urllib2.urlopen(req)
        return  connent