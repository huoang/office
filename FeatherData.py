#!/usr/bin/env python
#coding:utf-8



import pandas as pd
import feather as fd



class CsvData:
    
    def __init__(self,year,season,
                 path,ncol):
        self.year = year
        self.season = season
        self.path = path
        self.ncol = int(ncol)
        self.selvar = [1,5,2,31,33,34,35,59,
            64,69,70,71,72]+range(229,259)
        #self.writepath = '/mnt/e/pyr/data/2015/'
    
    def dfvars(self):
        dfvars=''
        for var in range(1,self.ncol+1):
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars
    
    
    
    def selvars(self):
        dfvars=''
        for var in self.selvar:
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
                fd.write_dataframe(df_rep,
                       writepath+self.year+'_'+
                       self.season+'.pyr'+
                       str(looptimes))   
            except StopIteration:
                loop =False
                print"Iteration is stopped."
    
    
        
        
if __name__ == '__main__':
    
    writepath = '/mnt/e/pyr/data/2015/'
    
    data1501 = CsvData('2015','01',
                '/mnt/f/data/2015/1501.CSV',
                261)    
    
    data1501.writedf(50000,writepath)
    
    data1502 = CsvData('2015','02',
                '/mnt/f/data/2015/1502.CSV',
                261)    
    
    data1502.writedf(50000,writepath)
    
    
    data1503 = CsvData('2015','03',
                '/mnt/f/data/2015/1503.CSV',
                261)    
    
    data1503.writedf(50000,writepath)
     
    data1504 = CsvData('2015','04',
                '/mnt/f/data/2015/1504.CSV',
                261)    
    
    data1504.writedf(50000,writepath) 
'''
def dfvars(ncol):
        dfvars=''
        for var in range(1,ncol+1):
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars
type(data1501.ncol)
type(data1501.dfvars())
data1501.selvars()
data1501.selvar

def selvars(selvar):
        dfvars=''
        for var in selvar:
            dfvars+='x%d,' %var
        dfvars=dfvars[:len(dfvars)-1]
        return dfvars

data1501.reader().get_chunk(1000)

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


'''        
  



        
        