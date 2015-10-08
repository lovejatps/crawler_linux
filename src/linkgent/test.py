# !/usr/bin/eny python
# -*- coding:utf-8 -*-
import sys
import threading
reload(sys)
sys.setdefaultencoding('utf-8')
# 
# name = raw_input('please ether your name:')
# print  'hollo world:', name 
# 
# srt1 = 'I 中国人'
# 
# srt1.encode('utf-8')
# print srt1.decode('utf-8')


def  test_threading():
    for i in range(10):
        print 'test_threading' ,i


print 'main  threading'

t1 = threading.Thread(target=test_threading)
t1.setDaemon(False)
t1.start()


