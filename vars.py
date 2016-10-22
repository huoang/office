#!/usr/bin/env python
#encoding:utf-8


from cio import load_pkl, save_pkl
import csv
import pandas as pd
from pandas.core.frame import DataFrame


hoscode=load_pkl("D:/git/getrec2/","hoscode.pkl")
print len(hoscode)


hosnames = pd.read_csv(r'E:/R/rdoc/hosnames.csv',
                         encoding="utf-8")
df_hosnames=DataFrame(hosnames)

print type(df_hosnames)
print df_hosnames  
df_hosnames.columns='code','names'
save_pkl(df_hosnames,'d:/git/getrec2/','df_hosnames.pkl')

hoscodes15=[int(code) for code in hoscode]
hosdic=dict(zip(hosnames.code,hosnames.names))
hosnames15=[hosdic[key] for key in hosdic
           if key in hoscode]
hoscodes15=[key for key in hosdic 
             if key in hoscode]
hos15=pd.Series(hosnames15,hoscodes15)
save_pkl('d:/git/getrec2','hos15.okl')


