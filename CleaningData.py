#!/usr/bin/env python
#coding:utf-8



import pandas as pd
import feather as fd
import os


path = '/mnt/e/pyr/data/2015x/'
xfiles = os.listdir(path)
xfiles.sort()
xfiles.sort(key=len)
len(xfiles)
df229 =  fd.read_dataframe(path+xfiles[12])
df262 = fd.read_dataframe(path+xfiles[42])
df262 = df262.set_index('x5')
df262.x262 = df262.x262.astype('str')
veri_true = df262.x262.isin(['True'])
df262[veri_true].index.value_counts()

#410000001034
path = '/mnt/e/pyr/data/2015perhos/'

wrong_fee_df = fd.read_dataframe(path+'410000003338_2015.pyr')

wrong_fee_df.ix[:,'x235']





