#!/usr/bin/env python
#encoding:utf-8

from cio import load_pkl
import pandas as pd
import re
import os

def dfvars(a_df):
    dfvars=''
    for var in range(1,len(a_df.columns)+1):
        dfvars+='x%d,' %var
    dfvars=dfvars[:len(dfvars)-1]
    return dfvars
def rep_str(m):
    rep=m.group()
    rep=rep.replace(',','|')
    return rep

def getdf(list_files):
    dfcols=[]
    for file in list_files:
        hos=pd.read_csv(u'e:/ipy/procdata/rec15/'+
            file.decode('gbk'),header=None,
            names=['cont'],encoding='utf-8')
        lst_hos=list(hos.cont)
        lst_hos=[line.encode('utf-8') for line in lst_hos] 
        lst_hos=[re.sub('\"+.+?\"+',rep_str,line) for line
                  in lst_hos]
        str_hos=[line.split(',') for line in lst_hos]
        df_hos=pd.DataFrame(str_hos)
        vars=dfvars(df_hos)
        df_hos.columns=[vars.split(',')]
        dfcols+=[len(df_hos.columns)]
        df_hos.to_csv(u'e:/ipy/procdata/rlt15/'+
            file.decode('gbk'),index=False,
            encoding='utf-8')
    return dfcols


files=os.listdir(r'E:/ipy/procdata/rec15')
files=files[1:]
dfcols15=getdf(files)

len(files)

'''
type(files)
for file in files:
    print file.decode('gbk')

hos=pd.read_csv(u'e:/ipy/procdata/rec15/'+
        files[0].decode('gbk'),header=None,
     names=['cont'],encoding='utf-8')

print files[0].decode('gbk')

df_zd2=pd.DataFrame(str_zd2)



len(vars)
len(df_zd2)

vars.__class__


str(df_zd2.ix[39707])



print lst_zd2[39707]

m=re.findall('".+?"',lst_zd2[39705])



print m




print lst_zd2_rplcomma[39707]
str_zd2_rplcomma=[line.split(',') 
                  for line in lst_zd2_rplcomma]

df_zd2_rplcomma=pd.DataFrame(str_zd2_rplcomma)

df_zd2_rplcomma.ix[df_zd2_rplcomma.x261!='\r\r\n']

set(df_zd2_rplcomma.x261)

lst_hos=[re.sub('".+?"',rep_str,line)
            for line in lst_hos]

zd2=pd.read_csv(u'e:/ipy/procdata/rec15/'+
            u'安阳市滑县人民医院_y15.csv',
            header=None,names=['cont'],
            encoding='utf-8')
lst_zd2=list(zd2.cont)
lst_zd2=[line.encode('utf-8') for line in lst_zd2] 
lst_zd2=[re.sub('\"+.+?\"+',rep_str,line) for 
    line in lst_zd2]
str_zd2=[line.split(',') for line in lst_zd2]
        df_zd2=pd.DataFrame(str_zd2)
vars=dfvars(df_zd2)
df_zd2.columns=[vars.split(',')]

df_zd2.to_csv(u'e:/ipy/procdata/'+
            'hx.csv',index=False,
            encoding='utf-8')
print lst_zd2[7991]

len(lst_zd2)
'''