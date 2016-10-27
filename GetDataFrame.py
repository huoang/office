#!/usr/bin/env python
#encoding:utf-8

from cio import load_pkl,save_pkl
import pandas as pd
from pandas.io.parsers import read_csv
from pandas.core.strings import str_split
import string



zd2=load_pkl('E:/ipy/procdata/rec15/',
         u'郑州大学第二附属医院_y15.pkl')
zd2=pd.read_csv('E:/ipy/procdata/rec15/'+
         u'郑州大学第二附属医院_y15.pkl',encoding='utf-8')

zd2=pd.read_csv(u'e:/ipy/procdata/rec15/'+
u'郑州大学第二附属医院_y15.csv',header=None,
     names=['cont'],encoding='utf-8')

type(zd2.cont)
zd2.ix('880')


zd2=list(zd2.cont)


zd2=[line.encode('utf-8') for line in zd2] 
print zd2[0],zd2[1],zd2[2]

str_zd2=[line.split(',') for line in zd2]
type(zd2)
len(zd2)
df_zd2=pd.DataFrame(str_zd2)

a=''
for i in range(len(df_zd2.columns)):
     a+='x%d,' %i
a=a[:len(a)-1]
=

df_zd2.columns=[a.split(',')]


