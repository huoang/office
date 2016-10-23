#!/usr/bin/env python
#encoding:utf-8


import numpy as np
import re
import codecs
from compiler.ast import flatten
#import pickle
from cio import save_pkl 

def getrec(strpath):
    f=codecs.open(strpath,encoding='utf-8')
    full=f.readlines()
    hoscode=map((lambda x:re.findall("41\\d{10}",
                    x[45:75])),full)
    hoscode=flatten(hoscode)
    f.close()
    return(hoscode)

hoscode1501=getrec('e:/data/2015/1501.csv')
hoscode1502=getrec('e:/data/2015/1502.csv')
hoscode1503=getrec('e:/data/2015/1503.csv')
hoscode1504=getrec('e:/data/2015/1504.csv')
hoscode=hoscode1501+hoscode1502+hoscode1503+hoscode1504
hoscode=np.array(hoscode) 
hoscode=set(hoscode)
save_pkl(hoscode,'d:/git/getrec2/', 'hoscode.pkl')



