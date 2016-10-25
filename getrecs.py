#!/usr/bin/env python
#encoding:utf-8


import numpy as np
import codecs
import csv
from cio import save_pkl , load_pkl
import re
import pandas as pd
from pandas.core.strings import str_split
import global_list as gl


def getfullrecs(strpath):
    f=codecs.open(strpath,encoding='utf-8')
    fullrecs=f.readlines()
    f.close()
    ix_s1=map((lambda x:re.findall("41\\d{10}",x[45:75])),
          fullrecs)
    ix_s1_d=[]
    for line in ix_s1:
        if line==[]:
            line='header'
            ix_s1_d.append(line)
        else:
            ix_s1_d.append(line)
    Ser1501=pd.Series(fullrecs,index=ix_s1_d)
    ix_s1_exclude_header=[]
    for line in ix_s1_d:
        if line=='header':
            pass
        else:
            ix_s1_exclude_header.append(line)
    hoscode=load_pkl('e:/ipy/getrec/', 'hoscode.pkl')
    hos_code=[(line,) for line in hoscode]
    hosdic=load_pkl('e:/ipy/getrec/','hos15.pkl')
    hosdicix=hosdic.index
    hosdicix=tuple(hosdicix)
    hosdicix=[str(line) for line in hosdicix]
    hosdicix=[(line,) for line in hosdicix]
    hosdic.index=hosdicix
    save_pkl(hosdic,'e:/ipy/getrec/','hos15_dic.pkl')
    for line in hos_code:
        try:
            hos=Ser1501[line]
            hos.to_csv('e:/ipy/procdata/hos15/'+
            hosdic[line]+'_s'+strpath[13:17]+
            '.csv',encoding='utf-8')
        except:
            continue
    
    



