#!/usr/bin/env python
#encoding:utf-8


import numpy as np
import codecs
import csv
from cio import save_pkl , load_pkl
import re
import pandas as pd
from pandas.core.strings import str_split
import string


def getfullrecs(strpath):
    f=codecs.open(strpath,encoding='utf-8')
    fullrecs=f.readlines()
    f.close()
    return(fullrecs)

recs1501=getfullrecs('f:/data/2015/1501.csv')
'''
recs1502=getfullrecs('e:/data/2015/1502.csv')
recs1503=getfullrecs('e:/data/2015/1503.csv')
recs1504=getfullrecs('e:/data/2015/1504.csv')
recs15=recs1501+recs1502+recs1503+recs1504
save_pkl(recs15,'f:/eps/getrec2/', 'fullrecs15.pkl')
'''

ix_s1=map((lambda x:re.findall("41\\d{10}",x[45:75])),
          recs1501)
ix_s1_d=[]
for line in ix_s1:
    if line==[]:
        line='header'
        ix_s1_d.append(line)
    else:
        ix_s1_d.append(line)

Ser1501=pd.Series(recs1501,index=ix_s1_d)
save_pkl(Ser1501,'f:/eps/getrec2/','ser1501.pkl')
##index part
ix_s1_exclude_header=[]
for line in ix_s1_d:
    if line=='header':
        pass
    else:
        ix_s1_exclude_header.append(line)



ix_s1_tuple=[tuple(line) for line in ix_s1_exclude_header]

len(Ser1501['410000000624',])

len(ix_s1_exclude_header)
ix_s1_exclude_header.count('201501')


ix_s1_tuple=tuple(ix_s1_tuple)
ix_s1_tuple[1]
len(ix_s1_tuple)
hos_code=[list(line) for line in hoscode]
hos_code=list(hoscode)
tuple(hoscode)
pd.core.strings.str_split(hoscode)
len(hoscode)
hoscode[1]
type(hoscode)
hos_code[0]
ix_s1_tuple[0] in 
(hos_code[0],) in ix_s1_tuple
hos_code=''.join(hos_code)
Ser1501.index[1]
ix_s1_tuple[0] in hos_code
hos_code=[(line,) for line in hos_code]
len(Ser1501[hos_code[0]])
hosdic=load_pkl('f:/eps/getrec2/','hos15.pkl')
type(hosdic)
hosdic[hos_code[5]]
hosdic[hosdicix[10]]
hosdicix=hosdic.index
hosdicix=tuple(hosdicix)
hosdicix=[str(line) for line in hosdicix]
hosdicix=[(line,) for line in hosdicix]
(hosdicix[6],) in hos_code
hosdicix
hosdic.index=hosdicix
hosdic.index[0]
Ser1501[hos_code[0]]

for line in hos_code:
    try:
        hos=Ser1501[line]
        hos.to_csv('F:/eps/getrec2/hos15/'+
               hosdic[line]+'.csv',encoding='utf-8')
    except:
        continue
    
    



