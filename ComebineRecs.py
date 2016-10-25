#!/usr/bin/env python
#encoding:utf-8



from global_list import strpath
import pandas as pd
from cio import load_pkl, save_pkl
from vars import hosdic
import os
from compiler.pycodegen import EXCEPT

hoscode=load_pkl('e:/ipy/getrec/', 'hoscode.pkl')
hos_code=[(line,) for line in hoscode]
hosdic15=load_pkl('e:/ipy/getrec/','hos15_dic.pkl')
'''
while(hos_code):      
    for path in strpath:
        try:
            hoscsv=pd.read_csv(u'e:/ipy/procdata/hos15/'+
            hosdic15[line]+u'_s'+path[13:17]+
                       u'.csv',header=None,names=['ixcol','content'],
                       encoding='utf-8') 
            hoscsv=list(hoscsv.content)
            hoscsv+=hoscsv
        except:
            continue
    save_pkl(hoscsv,'e:/ipy/procdata/',
                hosdic15[line]+'_y15.pkl')
'''    
lpcontrol=bool(1)
while(lpcontrol):
        for line in hos_code:      
            try:    
                for path in strpath:
                    try:
                        hoscsv=pd.read_csv(u'e:/ipy/procdata/hos15/'+
                                           hosdic15[line]+u'_s'+path[13:17]+
                                           u'.csv',header=None,names=['ixcol','content'],
                                           encoding='utf-8') 
                        hoscsv=list(hoscsv.content)
                        hoscsv+=hoscsv
                    except IOError:
                        continue  
        save_pkl(hoscsv,'e:/ipy/procdata/',
                hosdic15[line]+'_y15.pkl')
            except  KeyError:
                continue
lpcontrol=bool(0)            

     
        
        
        
'''        
hosdic15[(u'410000750010',)]   
type(hos_code)
type(hosdic15)
type(strpath)
strpath[]



if os.path.exist(filename)
    os.remove(filename)       
        
        
save_pkl(hoscsv,'e:/ipy/procdata/hos15',
                 hosdic15[hos_code[22]]+'_y15.pkl')


hoscsv=pd.read_csv(u'e:/ipy/procdata/hos15/'+
            hosdic15[hos_code[0]]+u'_s'+strpath[0][13:17]+
            u'.csv',header=None,names=['ixcol','content'],
            encoding='utf-8')
hoscsv1=pd.read_csv(u'e:/ipy/procdata/hos15/'+
            hosdic15[hos_code[22]]+'_s'+strpath[1][13:17]+
            '.csv',header=None,names=['ixcol','content'],
            encoding='utf-8')

hoscsv=list(hoscsv.content)
hoscsv1=list(hoscsv1.content)
hoscsv2=hoscsv+hoscsv1
len(hoscsv2)

hoscode=load_pkl('e:/ipy/getrec/', 'hoscode.pkl')
hos_code=[(line,) for line in hoscode]
hosdic=load_pkl('e:/ipy/getrec/','hos15.pkl')
hosdicix=hosdic.index
hosdicix=tuple(hosdicix)
hosdicix=[str(line) for line in hosdicix]
hosdicix=[(line,) for line in hosdicix]
hosdic.index=hosdicix
save_pkl(hosdic,'e:/ipy/getrec/','hos15_dic.pkl')


hosdic15.index
type(hosdic15)
type(hoscode)
'''
