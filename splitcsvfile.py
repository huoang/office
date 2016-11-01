#!/usr/bin/env python
#coding:utf-8

import pandas as pd
import os
from cio import load_pkl,save_pkl


def dfvars(a_df):
    dfvars=''
    for var in range(1,len(a_df.columns)+1):
        dfvars+='x%d,' %var
    dfvars=dfvars[:len(dfvars)-1]
    return dfvars
vars=dfvars(hos)

reader =pd.read_csv('f:/data/2013/1301.csv',
                     iterator=True)
loop = True
chunkSize = 1000
hoscodes = []
looptimes=0
while loop:
    try:
        df =reader.get_chunk(chunkSize)
        df.columns=vars.split(',')
        #hoscodes+=[df.icol(4).values]
        looptimes+=1
        hosix=df.icol(4).values
        hosix=set(hosix)
        hosix=[code for code in hosix]
        for code in hosix:
            df_rep=df.ix[df.x5==code,:]    
            df_rep.to_csv('e:/ipy/procdata/temp1/'
                          +str(code)+'_lp'+str(looptimes)+'.csv',
                        encoding='utf-8')
    except StopIteration:
        loop =False
        print"Iteration is stopped."

hos=pd.DataFrame([range(261)]).drop(0,axis=0)
vars=dfvars(hos)
hos.columns = vars.split(',')
files = os.listdir('e:/ipy/procdata/temp1') 
codes = [file[0:12] for file in files]
codes = set(codes)
codes = [code for code in codes]
codes.sort()
files.sort()
for code in codes:
    for file in files:
        if file[0:12] == code:
            hos_rep = pd.read_csv('e:/ipy/procdata/temp1/'
                          +file,header=None,
                          encoding='utf-8')
            hos=pd.concat([hos,hos_rep],axis=1)
    hos_rep.to_csv('e:/ipy/procdata/temp2/'
                    +str(code)+'.csv',
                        encoding='utf-8')
type(hos)
codes = codes.sort()
print codes

codes[1]==files[8][0:12]

'''
hoscodes=[code for list in hoscodes for code in list]
len(hoscodes)
hoscodes13=set(hoscodes)
hoscodes13=[int(code) for code in hoscodes13]
save_pkl(hoscodes13,'e:/ipy/getrec/',
             'hoscode13.pkl')


df.columns=vars.split(',')
df=df.set_index('x5')

'''











