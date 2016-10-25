#!/usr/bin/env python
#encoding:utf-8


from cio import load_pkl, save_pkl
import csv
import pandas as pd
from pandas.core.frame import DataFrame


hoscode=load_pkl("e:/ipy/getrec/","hoscode.pkl")
print len(hoscode)
hosnames = pd.read_csv(r'e:/ipy/getrec/hosnames.csv',
                         encoding="gbk")
hosnames.columns='code','names'
hoscodes15=[int(code) for code in hoscode]
hosdic=dict(zip(hosnames.code,hosnames.names))
hosnames15=[hosdic[key] for key in hosdic
           if key in hoscodes15]
hoscodes15=[key for key in hosdic 
             if key in hoscodes15]
hos15=pd.Series(hosnames15,hoscodes15)
save_pkl(hos15,'e:/ipy/getrec/','hos15.pkl')


