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

writepath = '/mnt/e/pyr/data/2015/'
#def combinecol(ncol,writepath):
cfiles = os.listdir(writepath)
dfvars = vars.split(',')
single_var=['_'+var+'.' for var in 
                   dfvars[:258]]

for var in single_var:
    cols=[filename for filename in 
                  cfiles if var in filename]
    ccol = pd.DataFrame() 
    for col in cols:
        col_rep = fd.read_dataframe(writepath+col)
        ccol = pd.concat([ccol,col_rep],axis = 0)
    fd.write_dataframe(ccol,'/mnt/e/pyr/data/2015x/'+
                   '2015'+var+'pyr')  

loop = True
looptimes=0
reader = pd.read_csv( '/mnt/f/data/2015/1502.CSV',
                     iterator = True)
while loop:
    try:
        looptimes += 1
        df = reader.get_chunk(100)
        df.columns = dfvars(261).split(',')
        df_rep = df.ix[:,['x5','x1'][:]] 
        rec=df[df['x1']=='子']
        rec.x229

'''
#combinecol(261, writepath)
len(cfiles)
len(single_var)
len(ccol)
len(cols)

type(single_var)    

single_var[257]

cols
'''    
    
    