#!/usr/bin/env python
#encoding:utf-8


from cio import load_pkl, save_pkl
import csv
import pandas as pd
from pandas.core.frame import DataFrame


hoscode=load_pkl("f:/eps/getrec2/","hoscode.pkl")
print len(hoscode)


hosnames = pd.read_csv(r'f:/eps/getrec2/hosnames.CSV',
                         encoding="gbk")
'''
df_hosnames=DataFrame(hosnames)

print type(df_hosnames)

df_hosnames.columns='code','names'
print df_hosnames 
''' 
save_pkl(hosnames,'f:/eps/getrec2/','df_hosnames.pkl')

hoscodes15=[int(code) for code in hoscode]
hosdic=dict(zip(hosnames.code,hosnames.name))
hosnames15=[hosdic[key] for key in hosdic
           if key in hoscodes15]
hoscodes15=[key for key in hosdic 
             if key in hoscodes15]
hos15=pd.Series(hosnames15,hoscodes15)

save_pkl(hos15,'f:/eps/getrec2/','hos15.pkl')


