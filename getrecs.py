#!/usr/bin/env python
#encoding:utf-8


import numpy as np
import codecs
import csv
from cio import save_pkl 
import re
import pandas as pd

def getfullrecs(strpath):
    f=codecs.open(strpath,encoding='utf-8')
    fullrecs=f.readlines()
    f.close()
    return(fullrecs)

recs1501=getfullrecs('e:/data/2015/1501.csv')
'''
recs1502=getfullrecs('e:/data/2015/1502.csv')
recs1503=getfullrecs('e:/data/2015/1503.csv')
recs1504=getfullrecs('e:/data/2015/1504.csv')
recs15=recs1501+recs1502+recs1503+recs1504
#fullrecs15=np.array(recs15) 
save_pkl(recs15,'d:/git/getrec2/', 'fullrecs15.pkl')
'''

ix_s1=map((lambda x:re.findall("41\\d{10}",x[45:75])),recs1501)
ix_s1_d=[]
for line in ix_s1:
    if line==[]:
        line='header'
        ix_s1_d.append(line)
    else:
        ix_s1_d.append(line)

Ser1501=pd.Series(recs1501,index=ix_s1_d)
save_pkl(Ser1501,'d:/git/getrec2/','ser1501.pkl')

