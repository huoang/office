#!/usr/bin/env python
#coding:utf-8



import pandas as pd
import feather as fd
import os


class CsvData:
    
    
    
    def __init__(self,year,season,
                 path,ncol,ixcol):
        self.year = year
        self.season = season
        self.path = path
        self.ncol = int(ncol)
        self.selvar = [1,3,5,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)
        #self.writepath = '/mnt/e/pyr/data/2015/'
        self.ixcol = ixcol
        
        
    def dfvars(self):
        dfvars=''
        for var in range(1,self.ncol+1):
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars
    
    @staticmethod
    def s_dfvars(ncol):
        dfvars=''
        for var in range(1,ncol+1):
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars
    
    
    def selvars(self):
        dfvars=''
        for var in self.selvar:
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars
   
    @staticmethod   
    def s_selvars(selvar):
        dfvars=''
        for var in selvar:
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars    
    
    def writedf(self,chunkSize,writepath):
        loop = True
        looptimes=0
        reader = pd.read_csv(self.path,
                     iterator = True)
        while loop:
            try:
                looptimes += 1
                df = reader.get_chunk(chunkSize)
                df.columns = self.dfvars().split(',')
                df_rep = df.ix[:,
                    self.selvars().split(",")] 
                df_rep = df_rep.astype('str')
                for var in df_rep.columns:
                    try:
                        df_cols = df_rep[[self.ixcol,var]]
##                      df_cols = pd.DataFrame(df_cols)
                        fd.write_dataframe(df_cols,
                        writepath+self.year+'_'+
                             self.season+'_'+var+'.pyr'+
                             str(looptimes))
                    except ValueError:
                        continue   
            except StopIteration:
                loop =False
                print"Iteration is stopped."
    
    @classmethod
    def combinecol(clx,selvar,outputpath):
        cfiles = os.listdir(writepath)
        selvars = clx.s_selvars(selvar).split(',')
        single_var=['_'+var+'.' for var in 
                   selvars[:258]]
        for var in single_var:
            cols=[filename for filename in 
                  cfiles if var in filename]
            cols.sort()
            ccol = pd.DataFrame() 
            for col in cols:
                col_rep = fd.read_dataframe(writepath+col)
                ccol = pd.concat([ccol,col_rep],axis = 0)
            fd.write_dataframe(ccol,outputpath+'2015'
                               +var+'pyr')  
     
        
if __name__ == '__main__':
    '''
    writepath = '/mnt/e/pyr/data/2015/'
    
    data1501 = CsvData('2015','01',
                '/mnt/f/data/2015/1501.CSV',
                261,'x5')    
    
    data1501.writedf(50000,writepath)
  
    data1502 = CsvData('2015','02',
                '/mnt/f/data/2015/1502.CSV',
                261,'x5')    
    
    data1502.writedf(50000,writepath)
    
    
    data1503 = CsvData('2015','03',
                '/mnt/f/data/2015/1503.CSV',
                261,'x5')    
    
    data1503.writedf(50000,writepath)
     
    data1504 = CsvData('2015','04',
                '/mnt/f/data/2015/1504.CSV',
                261,'x5')    
    
    data1504.writedf(50000,writepath)
    
    selvar = [1,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)
    CsvData.combinecol(selvar,'/mnt/e/pyr/data/2015x/')
    
    print 'mission accomplished!!!'
    '''
    writepath = '/mnt/e/pyr/data/2014/'
    
    data1401 = CsvData('2014','01',
                '/mnt/f/data/2014/1401.csv',
                239,'x3')    
    
    data1401.writedf(50000,writepath)
  
    data1402 = CsvData('2014','02',
                '/mnt/f/data/2014/1402.csv',
                239,'x3')    
    
    data1402.writedf(50000,writepath)
    
    
    data1403 = CsvData('2014','03',
                '/mnt/f/data/2014/1403.csv',
                239,'x3')    
    
    data1403.writedf(50000,writepath)
     
    data1404 = CsvData('2014','04',
                '/mnt/f/data/2014/1404.csv',
                239,'x3')    
    
    data1404.writedf(50000,writepath)
    
    selvar = [1,2,12,14,15,16,40,
            45,50,51,52,53]+range(210,240)
    CsvData.combinecol(selvar,'/mnt/e/pyr/data/2014x/')
    
    print 'mission accomplished!!!' 
     
     
     
'''
s_selvars(selvar)
df['x239']
def dfvars(ncol):
        dfvars=''
        for var in range(1,ncol+1):
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars
vars=dfvars(261)
type(data1501.ncol)
type(data1501.dfvars())
data1501.selvars()
data1501.selvar

vars = s_dfvars(261)
type(vars)
dfvars = CsvData.s_dfvars(261).split(',')
type(str(dfvars))
dfvars.split(',')
type(dfvars)
str(dfvars)
type([var for var in str(dfvars)])
single_var=['_'+var+'.' for var in 
                   dfvars.split(',')[:258]]

dfvars = CsvData.s_dfvars(261).split(',')
type(dfvars)
single_var=['_'+var+'.' for var in 
                   dfvars[:258]]



def selvars(selvar):
        dfvars=''
        for var in selvar:
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars

def selvars(selvar):
        dfvars=''
        for var in selvar:
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars

selvar = [1,5,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)

seldvar=selvars(selvar)
data1501.reader().get_chunk(1000)

path='/mnt/f/data/2015/1501.CSV'

reader = pd.read_csv(path,
                     iterator = True)

df = reader.get_chunk(50000)

df.columns = vars.split(',')
df_rep = df.ix[:,seldvar.split(",")] 
df_rep = df_rep.astype('str')

df_col=df_rep[df_rep.columns[0]]
df_col=pd.DataFrame(df_col)
fd.write_dataframe(ccol,
    '/mnt/e/pyr/data/'+'1501'+'_'+
    '01'+'.pyr')   

CsvData.dfvars().split(',')

reader._currow = 0

files=os.listdir(writepath)

df = fd.read_dataframe(writepath+files[0])

def writecols(self):
        files=os.listdir(self.writepath)
        for file in files:
            df = fd.read_dataframe(writepath+files)
                for 
        df[df.columns[0]]

    print var 
reader = pd.read_csv('/mnt/f/data/2014/1403.csv',
                     iterator = True)
df = reader.get_chunk(100)

vars = dfvars(239)
df.columns = vars.split(',')



'''        
  



        
        