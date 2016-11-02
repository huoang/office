#!/usr/bin/env python
#coding:utf-8

import pandas as pd
import os
from cio import load_pkl,save_pkl
import feather

def dfvars(a_df):
    dfvars=''
    for var in range(1,len(a_df.columns)+1):
        dfvars+='x%d,' %var
    dfvars=dfvars[:len(dfvars)-1]
    return dfvars
vars=dfvars(df)



reader = pd.read_csv('f:/data/2013/1301.csv',
                     iterator=True)
reader._currow = 0

loop = True
chunkSize = 50000
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
                index=None,encoding='utf-8')
    except StopIteration:
        loop =False
        print"Iteration is stopped."
        
        
df.to_csv('e:/ipy/procdata/'+'sample.csv',index=None)        

path='e:/ipy/procdata/'+'sample.pyr'
fd.write_dataframe(df,path) 
df = feather.read_dataframe(path)

        
        