#!/usr/bin/env python
#coding=utf-8

import logging
import math
from __builtin__ import str
from _ast import Str

def Logg(object):
    logger = logging.getLogger()
    file = logging.FileHandler('qqxml.log')
    logger.addHandler(file)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file.setFormatter(formatter)
    logger.setLevel(logging.NOTSET)


for i in range(0,10):
   Logg(Str(i))
