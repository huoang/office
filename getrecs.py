#!/usr/bin/env python
#encoding:utf-8


import re
import codecs
from cio import save_pkl , load_pkl
import pandas as pd
from global_list import list_path
import gethoscode as gh
from compiler.ast import flatten
from __builtin__ import isinstance

year=gh.getyear(list_path)
def gethosdic():
    hoscode=load_pkl('e:/ipy/getrec/', 
                     'hoscode'+year+'.pkl')
    hos_code=[int(line) for line in hoscode]
    hosall = pd.read_csv(u'e:/ipy/getrec/'+
                     u'hosnames.csv',encoding="utf-8")
    hosall.columns='code','names'
    hosdic=dict(zip(hosall.code,hosall.names))
    hos_name_cur_year=[hosdic[key] for key in 
            hosdic if key in hos_code]
    hos_code_cur_year=[key for key in hosdic 
             if key in hos_code]
    hos_cur_year=pd.Series(hos_name_cur_year,
            index=hos_code_cur_year)
    save_pkl(hos_cur_year,
          'e:/ipy/getrec/','hos'+year+'_dic.pkl')
    
def getfullrecs(strpath):
    if year=='14':
        start=1
        end=30
    elif year=='15':
        start=45
        end=75
    elif year=='13':
        start=45
        end=75
    else:
        print u'请确定年份是否正确！'
    hoscode_year=[]
    for path in strpath:
        file=codecs.open(path,encoding='utf-8')
        full=file.readlines()
        ix_s=map((lambda x:re.findall("41\\d{10}",
                    x[start:end])),full)
        ix_s_d=[]
        for line in ix_s:
            if line==[]:
                line='header'
                ix_s_d.append(line)
            else:
                ix_s_d.append(line)
        Ser=pd.Series(full,index=ix_s_d)
        ix_s_exclude_header=[]
        for line in ix_s_d:
            if line==['header']:
                continue
            elif len(line)>1:
                line==line[1]
            else:
                ix_s_exclude_header+=line
        hos_code=tuple(ix_s_exclude_header)  
        hos_code=set(hos_code)  
        hos_code=tuple(hos_code) 
        hoscode_year+=hos_code 
        for line in hos_code:
            try:
                hos=Ser[(line,)]
                if isinstance(hos,unicode):
                    hos=pd.Series(hos)
                hos.to_csv('e:/ipy/procdata/temp/'+
                        line+'_s'+
                        path[13:17]+'.csv',
                        encoding='utf-8')
            except KeyError:
                continue
    file.close()
    hoscode_year=set(hoscode_year)
    hoscode_year=list(hoscode_year)
    save_pkl(hoscode_year,'e:/ipy/getrec/',
             'hoscode'+year+'.pkl')
    
getfullrecs(list_path)



'''

hoscode15=load_pkl('e:/ipy/getrec/','hoscode15.pkl')
len(hoscode15)
file=codecs.open(list_path[1],encoding='utf-8')
len(full)
len(ix_s)
len(ix_s_d)
len(ix_s_exclude_header)
tuple(ix_s_exclude_header)
len(hoscode_year)
type(hos_code)
len(Ser)
hoscode=map((lambda x:re.findall("41\\d{10}",
                    x[start:end])),full)
if year=='14':
        start=1
        end=30
elif year=='15':
        start=45
        end=75
elif year=='13':
        start=45
        end=75
else:
    print u'请确定年份是否正确！'
Ser.index
hos=Ser[(hos_code[1],)]
hos=pd.Series(hos)
hos=Ser[(hos_code[10],)]
len(hos)
type(hos)=='unicode'
isinstance(hos,unicode)
hos.to_csv('e:/ipy/procdata/temp/'+
                hos_code[13]+'_s'+
                '1501'+'.csv',header=False,
                encoding='utf-8')
(hos_code[13],) in Ser.index
Ser.index[(u'410000750010',)]

type(Ser.index)
hos_code.index(u'410000750010')
Ser[389781]
hos_code[99]
type(hos_code)
hos_code=list(hos_code)
hos_code[0]
hos_code[1]
type(ix_s_d)
ix_s_d.index(u'410000750010')
ix_s.index(u'410000750010')
ix_s_d.index([u'410000750010'])
ix_s_exclude_header.index(u'410000750010')
ix_s_exclude_header.index(u'410000750010')
ix_s_exclude_header[389780]



data=[100, 94, 88, 82, 76, 70, 64, 58, 52, 46, 40, 34]
data.index(76)

ix_s_exclude_header=[]
ix_s_exclude_header=[]
for line in ix_s_d:
    ix_s_exclude_header+=line
len(ix_s)
len(ix_s_d)
len(ix_s_d2)
len(ix_s_exclude_header)
ix_s_d['header']

len(ix_s_d)-len(ix_s_exclude_header)
len(full)

line=(ix_s_d[0])
len(line)>1



ix_s_d2=[]
for line in ix_s_d:
    ix_s_d2+=line
ix_s_d2
ix_s_d2.index(u'410000750010')
ix_s_d2[399039]
full[391326]
ix_s[399039]
ix_s[0:5]
ix_s_d2[0:5]

ix_s_d=[]
for line in ix_s:
    if line==[]:
        line=['header']
        ix_s_d.append(line)
    else:
        ix_s_d.append(line)


ix_s[0:5]
len(ix_s_d[0])
ix_s_d[0:5]
ix_s_d2[0:5]
ix_s[1337614]
ix_s_d[1337614]
ix_s_d2[1337615]

ix_s_exclude_header[0:5]
ix_s_d.index([u'410000750010'])
ix_s.index([u'410000750010'])
ix_s_d.index([u'410000750010'])
ix_s_exclude_header.index(u'410000750010')
ix_s_exclude_header.index(u'410000750010')
ix_s_exclude_header[389781]
len(ix_s[391324])
len(ix_s_d[391324])

line=ix_s_d[391324]
line[1]
len(ix_s_d[391323])
compare(ix_s,ix_s_d)

tmp = [val for val in ix_s if val in ix_s_d]

ix_s1=[]
ix_s_d1=[]
for i in range(0,len(ix_s)):
    if ix_s[i] != ix_s_d[i]:
        ix_s1+=ix_s[i]
        ix_s_d1+=ix_s_d[i]
        
  len(ix_s_d1)  
  len(ix_s1)
  
