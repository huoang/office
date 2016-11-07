#!/usr/bin/env python
#coding:utf-8

import pandas as pd
import feather as ft



def dfvars(ncol):
    dfvars=''
    for var in range(1,ncol+1):
        dfvars+='x%d,' %var
    dfvars=dfvars[:len(dfvars)-1]
    return dfvars
vars=dfvars(261)


selcols=[1,5,31,33,34,35,59,
         64,69,70,71,72] + range(229,259)
def selvars(selcols):
    selvars=''
    for col in selcols:
        selvars+='x%d,'%col
    selvars=selvars[:-1]
    return(selvars)
selvars=selvars(selcols)

reader = pd.read_csv(
            '/mnt/f/data/2013/1301.csv',
                     iterator=True)
reader._currow = 0

loop = True
chunkSize = 50000
hoscodes = []
looptimes=0
path = '/mnt/e/ipy/procdata/2013/rec13.pyr'
while loop:
    try:
        looptimes+=1
        df =reader.get_chunk(chunkSize)
        df.columns=vars.split(',')
        df_rep=df.ix[:,selvars.split(',')]
        df_rep=df_rep.astype('str')     
        ft.write_dataframe(df_rep,path+
                           str(looptimes))
    except StopIteration:
        loop =False
        print"Iteration is stopped."
        
        
        
'''        
files = os.listdir('e:/ipy/procdata/2013') 
hos = pd.DataFrame()
for file in files:
    hos_rep = pd.read_csv('e:/ipy/procdata/2013/'
                          +file,header=None,
                          encoding='utf-8')
    hos = pd.concat([hos,hos_rep],axis=0)
  
hos.to_csv('e:/ipy/procdata/2013/'+
            'csv2013'+'.csv',
            index=None,encoding='utf-8')  
'''


 
        
        