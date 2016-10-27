#!/usr/bin/env python
#encoding:utf-8



from global_list import list_path
import pandas as pd
from cio import load_pkl



hoscode=load_pkl('e:/ipy/getrec/', 'hoscode.pkl')
hos_code=[(line,) for line in hoscode]
hosdic15=load_pkl('e:/ipy/getrec/','hos15_dic.pkl')
for line in hos_code:      
    try: 
        hoscsv_full=[]
        hosname=hosdic15[line]
    except  KeyError:
        line=[410000179612]  
        continue
    else:
        for path in list_path:
            try:
                hoscsv=pd.read_csv(u'e:/ipy/procdata/hos15/'+
                        hosname+u'_s'+path[13:17]+
                        u'.csv',header=None,names=['ixcol','content'],
                        encoding='utf-8') 
                hoscsv=list(hoscsv.content)
                hoscsv_full+=hoscsv
            except IOError:
                continue  
    finally:
        hoscsv_full=pd.Series(hoscsv_full)
        hoscsv_full.to_csv(u'e:/ipy/procdata/rec15/'+
                           hosname+u'_y'+path[13:15]+
                           u'.csv',index=False,
                           encoding='utf-8')
     
        
        
        
'''        
hosdic15[(u'410000750010',)]   
type(hos_code)
type(hosdic15)
type(strpath)
strpath[]
hosdic15
hos_code[0]
len(hos_code)
hosdic15[hos_code[0]]
type(hosdic15.index[0])
type(hos_code[0])
if os.path.exist(filename)
    os.remove(filename)       
        
        
save_pkl(hoscsv,'e:/ipy/procdata/hos15',
                 hosdic15[hos_code[22]]+'_y15.pkl')


hoscsv=pd.read_csv(u'e:/ipy/procdata/hos15/'+
            hosdic15[hos_code[0]]+u'_s'+list_path[0][13:17]+
            u'.csv',header=None,names=['ixcol','content'],
            encoding='utf-8')
hoscsv1=pd.read_csv(u'e:/ipy/procdata/hos15/'+
            hosdic15[hos_code[22]]+'_s'+strpath[1][13:17]+
            '.csv',header=None,names=['ixcol','content'],
            encoding='utf-8')

hoscsv_full[0:5].to_csv(u'e:/ipy/procdata/'+
                           u'zd1'+u'_y15'+
                           u'.csv',index=False,
                           encoding='utf-8')

hoscsv_full.values
hoscsv_full[0:5]

hoscsv_full[0:5][0]

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
