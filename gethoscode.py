#!/usr/bin/env python
#encoding:utf-8


import numpy as np
import re
import codecs
from compiler.ast import flatten
import pickle
from cio import save_pkl 
from global_list import list_path


def getrec(strpath):
    f=codecs.open(strpath,encoding='utf-8')
    full=f.readlines()
    hoscode=map((lambda x:re.findall("41\\d{10}",
                    x[45:75])),full)
    hoscode=flatten(hoscode)
    f.close()
    return(hoscode)
def getcode(listpath):    
    hoscode_full=[]
    for path in listpath:
        hoscode=getrec(path)
        hoscode=list(hoscode)
        hoscode_full+=hoscode 
    hoscode_full=set(hoscode_full)
    hoscode_full=list(hoscode_full)
    save_pkl(hoscode_full,'e:/ipy/getrec/','hoscode.pkl')



