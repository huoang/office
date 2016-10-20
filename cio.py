#!/usr/bin/env python
#encoding:utf-8



import pprint, pickle

def load_pkl(strpath,filename):
    pkl_file = open(strpath+filename,'rb')
    load_obj = pickle.load(pkl_file)
    #pprint.pprint(hoscode1501)
    print 'File_'+filename+'_successlly loaded!'
    return load_obj


      

def save_pkl(save_obj,strpath,filename):
    output=open(strpath+filename,'wb')
    pickle.dump(save_obj,output)
    print 'File_'+filename+'_succeselly saved!'


