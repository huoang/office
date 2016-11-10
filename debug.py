#!/usr/bin/env python
#coding:utf-8



import pandas as pd
import feather as fd
import os

def dfvars(ncol):
        dfvars=''
        for var in range(1,ncol+1):
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars

vars=dfvars(261)
ncol=261



loop = True
looptimes=0
reader = pd.read_csv( '/mnt/e/data/2015/1502.CSV',
                     iterator = True)
while loop:
    try:
        looptimes += 1
        df = reader.get_chunk(50000)
        df.columns = dfvars(261).split(',')
        df_rep = df[['x5','x1']] 
        rec=df[df['x1']=='子']
        df.ix[[24861]].x229
        df.ix[[24861]].x258

        df.ix[[24860]].x229
        df.ix[[24860]].x258
        
        df.ix[[24862]].x229
        df.ix[[24862]].x258
        
        df.ix[[24863]].x229
        df.ix[[24863]].x258
        
