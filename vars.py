#!/usr/bin/env python
#encoding:utf-8


from cio import load_pkl
import csv
import pandas as pd


hoscode=load_pkl("E:/ipy/getrec/","hoscode.pkl")
print len(hoscode)


hosnames = pd.read_csv(r'E:/R/rdata/hosnames.csv',
                         encoding="gbk")
print hosnames 
 

