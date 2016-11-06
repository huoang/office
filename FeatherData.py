#!/usr/bin/env python
#coding:utf-8

import pandas as pd
import feather as fd

def dfvars(ncol):
    dfvars=''
    for var in range(1,ncol+1):
        dfvars+='x%d,' %var
    dfvars=dfvars[:len(dfvars)-1]
    return dfvars
vars=dfvars(261)

def selvars(selvars):
    dfvars=''
    for var in selvars:
        dfvars+='x%d,' %var
    dfvars=dfvars[:len(dfvars)-1]
    return dfvars
selvar=[1,5,2,31]+range(229,259)
selvars=selvars(selvar)

###x1,x5,x2,x31,x33:x258



reader = pd.read_csv('/mnt/e/data/2013/1301.csv',
                     iterator=True)
reader._currow = 0

loop = True
chunkSize = 50000
hoscodes = []
looptimes=0
#df_rep=pd.DataFrame()
path='/mnt/e/ipy/procdata/2013/rec2013.pyr'
while loop:
    try:
        looptimes+=1
        df =reader.get_chunk(chunkSize)
        df.columns=vars.split(',')
        df_rep=df.ix[:,selvars.split(",")] 
        df_rep=df_rep.astype('str')
        fd.write_dataframe(df_rep,path+str(looptimes))   
    except StopIteration:
        loop =False
        print"Iteration is stopped."
        
        
      



        
        