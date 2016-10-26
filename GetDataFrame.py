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

zd2=pd.read_csv(u'e:/ipy/procdata/hos15'+
u'/郑州大学第二附属医院_s1501.csv',header=None,
     names=['ix','cont'],encoding='utf-8')

type(zd2.cont)

zd2=list(zd2.cont)

zd2=[line.encode('utf-8') for line in zd2] 
zd2[0]
type((zd2[0]))
str_zd2=list(zd2[0])
str_zd2=zd2[0:5]
print list(zd2[0])
print str_zd2
str_zd2=str_zd2.split(',')

print str_zd2[72]

pd.DataFrame(str_zd2)
type(str_zd2)

type(str_zd2)
type(zd2)

print zd2[2]


type(zd2)


len(zd2.index)
zd2.columns

zd2.index

zd2.count

type(zd2)
str_split[zd2[0]]
str_split(zd2[0:1])
zd2.columns
print zd2[0:10]
for line in zd2[0:10]:
    print line 

zd2[:0]
zd2.key
isinstance()
