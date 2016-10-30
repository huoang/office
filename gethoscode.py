#!/usr/bin/env python
#encoding:utf-8



import re
import codecs
from compiler.ast import flatten
from cio import save_pkl 
from global_list import list_path

def getyear(strpath):
    year=strpath[0][13:15]
    return year

def gethoscode(strpath):
    year=getyear(strpath)
    if year=='14':
        start=1
        end=30
    elif year=='15' or '13':
        start=45
        end=75
    else:
        print u'请确定年份是否正确！'
    f=codecs.open(strpath,encoding='utf-8')
    full=f.readlines()
    hoscode=map((lambda x:re.findall("41\\d{10}",
                    x[start:end])),full)
    hoscode=flatten(hoscode)
    f.close()
    return(hoscode)
def savecode(listpath):    
    year=getyear(listpath)
    hoscode_year=[]
    for path in listpath:
        hoscode=getrec(path)
        hoscode=list(hoscode)
        hoscode_year+=hoscode 
    hoscode_year=set(hoscode_year)
    hoscode_year=list(hoscode_year)
    save_pkl(hoscode_year,'e:/ipy/getrec/',
             'hoscode'+year+'.pkl')





