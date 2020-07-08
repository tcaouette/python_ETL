#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import numpy as np
import re
import sys
import csv
import openpyxl
import decimal
import os
from os import listdir
from os.path import isfile, join
import datetime as dt
import shutil
import uuid
from openpyxl import load_workbook
import camelot
import math
import pyodbc
import time
import sqlalchemy
from sqlalchemy import create_engine
import urllib
from tkinter import *
from tkinter import ttk, messagebox




pd.set_option('mode.chained_assignment', None)



def lookin_folder(root_dir1,root_dir2, r_exfile=None):# will have to account for another root_dir
    '''Looks in a folder via a path and builds a list with files that have extension pdf'''
    bld_files =[]
    bcw_files =[]
    root_bldfile=[]
    root_bcwfile=[]
    
    if len(os.listdir(root_dir1)) >= 1:
        for file in os.listdir(root_dir1):
            if file.endswith('.pdf'):
                bld_files.append(file)
                #ex_files = [i for i in ex_files if not i.startswith('~$')] # ----------- removes ~$ issue from list
                root_bldfile = [root_dir1 + i for i in bld_files]
    if len(os.listdir(root_dir2)) >= 1:
        for file in os.listdir(root_dir2):
            if file.endswith('.pdf'):
                bcw_files.append(file)
                root_bcwfile = [root_dir2 + i for i in bcw_files]
    files = root_bldfile + root_bcwfile
    return files
  


def extract_pdf(filelist): # need more functions to take care of the many possibilities to return dataframes
    
    if len(filelist) > 1:
#         for file in filelist:
#             if ".pdf" in file: # the string here will be augmented for each site once I have more pdfs
        tables_bld = camelot.read_pdf(filelist[0])
        df_bld = tables_bld[0].df
        df_bld = df_bld.iloc[2:]# remove the two stacked headers
        df_bld.columns=['1','2','3','4','5','6','7','8','9','10','11','12','13']
        df_bld.reset_index()
        df_bld = df_bld.replace('',np.nan)# turn the data into pandas dataframe 
        df_bld = df_bld[df_bld['3'].notna()]
        #print(df_bld)

        tables_bcw = camelot.read_pdf(filelist[1])
        df_bcw = tables_bcw[0].df
        df_bcw = df_bcw.iloc[2:]# remove the two stacked headers
        df_bcw.columns=['1','2','3','4','5','6','7','8','9','10','11','12','13']
        df_bcw.reset_index()
        df_bcw = df_bcw.replace('',np.nan)
        df_bcw = df_bcw[df_bcw['3'].notna()]
        #print(df_bcw)
        
        df_abid = pd.concat([df_bld, df_bcw])
        df_abid.reset_index()
        return df_abid

    elif len(filelist) == 1:
        #return print("less than 1")
        #for file in filelist:
           # if "Scrape" in file: # the string here will be augmented for each site once I have more pdfs
        tables = camelot.read_pdf(filelist[0])
        df_abid = tables[0].df
        
        df_abid = df_abid.iloc[2:]# remove the two stacked headers
        df_abid.columns=['1','2','3','4','5','6','7','8','9','10','11','12','13']
        df_abid.reset_index()
        df_abid = df_abid.replace('',np.nan)# turn the data into pandas dataframe 
        df_abid = df_abid[df_abid['3'].notna()]
        return df_abid
            

    else:
        print('There are no PDF files')



#df

def send_to_excel(df_abid, new_xlsx, text):
    add_date = dt.datetime.now().strftime("%Y%m%d_%H:%M:%S")
    try:
        with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
            file_object.seek(0)
    # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
    # Append text at the end of file
            yay = "Scrape Completed and Excel File Generated " + add_date
            file_object.write(yay)
        
        df_abid.columns = ['rowid',	'sample_ID',	'subj_age',	'subj_sex',	'sample_antibodies',	'samp_type',	'anticoagulant',	'collect_date',	'storage_temp',	'enroll_date',	'enrolled_by',	'comments',	'all_tests_excluded']
        first_row = pd.DataFrame([{'rowid':-1,'sample_ID':'XXXXXXXX','subj_age':'XXX',	'subj_sex':'XXX',	'sample_antibodies':'XXXXXXXXXXXXX',	'samp_type':'XXXXXX',	'anticoagulant':'XXXXXXXXXXX',	'collect_date':'1/1/2001',	'storage_temp':'XXXX',	'enroll_date':'1/1/2001',	'enrolled_by':'xxxxx',	'comments':'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',	'all_tests_excluded':'XXX'}])	
    
        df_abid = pd.concat([first_row, df_abid])
        df_abid = df_abid.reset_index(drop=True)
        df_abid.to_excel(new_xlsx, index=False)
        
        return print("Excel Doc Created")# need to add a header to the dataframe before it gets exported 
    except:
        with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
            file_object.seek(0)
    # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
    # Append text at the end of file
            failed = "Scrape Failed and No Excel File Created " + add_date
            file_object.write(failed)

def send_to_sql(df_abid,table_name,engine,text):
    #try:
    
    add_date = dt.datetime.now().strftime("%Y%m%d_%H:%M:%S")
    df_abid['dateinsert'] = pd.to_datetime('today')
    df_abid.to_sql(table_name,engine,if_exists='replace',index=False,dtype={'rowid':sqlalchemy.types.INTEGER(),
                                                                        'sample_ID':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'subj_age':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'subj_sex':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'sample_antibodies':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'samp_type':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'anticoagulant':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'collect_date':sqlalchemy.types.DATE(),
                                                                        'storage_temp':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'enroll_date':sqlalchemy.types.DATE(),
                                                                        'enrolled_by':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'comments':sqlalchemy.types.NVARCHAR(length=4000),
                                                                        'all_tests_excluded':sqlalchemy.types.NVARCHAR(length=50),
                                                                      })
    time.sleep(10)#pauseing for 10 seconds to ensure import has finished so archive can happen without error
    with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
        file_object.seek(0)
    # If file is not empty then append '\n'
        data = file_object.read(100)
        if len(data) > 0 :
            file_object.write("\n")
    # Append text at the end of file
        excellent = "Data Successfully Appended to SQL Table " + add_date
        file_object.write(excellent)
   # except:
       # print("Check Database Connection")

# move excel file before writing new excel file and move pdf's to new folder
def move_files(root_dir1, root_dir2,root_dir3, dest_folder):
    '''root_dir1 is the directory of bcw and root_dir1 directory bld, dest_folder is archive for both pdf and excel files'''
    pdf_bcw_files =[]
    pdf_bld_files=[]
    excel_file=[]
    root_files1=[]
    root_files2=[]
    root_files3=[]
    add_date = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    print(add_date)
    #if file in listdir(root_dir1):
    if len(listdir(root_dir1)) >= 1:
        for file in listdir(root_dir1):
            if file.endswith('.pdf'):
                pdf_bcw_files.append(file)
                root_files1 = [root_dir1 + i for i in pdf_bcw_files]
    if len(listdir(root_dir2)) >= 1:
        for file in listdir(root_dir2):
            if file.endswith('.pdf'):
                pdf_bld_files.append(file)
                root_files2 = [root_dir2 + i for i in pdf_bld_files]
    if len(listdir(root_dir3)) >= 1:
        for file in listdir(root_dir3):
            if file.endswith('.xlsx'):
                excel_file.append(file)
                root_files3 = [root_dir3 + i for i in excel_file]
                
    files = root_files1 + root_files2 + root_files3
   # print(files)
    try:
        if len(files) > 0:
    
            for file in files:
                shutil.move(file, dest_folder)
    
            for file in listdir(dest_folder):
                if file.endswith('.xlsx'):# if file ends with .pdf or xlsx, then remove the .xlsx and .pdf from name then create new name append the xlsx and pdf back
                    if not file.startswith("arc"):
                        dst = dest_folder + 'arc_'+   file[:-5] +'_' +  add_date +'.xlsx'
                        src =  dest_folder + file
        
                        os.rename(src, dst)
                if file.endswith('.pdf'):
                    if not file.startswith("arc"):
                        dst = dest_folder + 'arc_'+   file[:-4] +'_' +  add_date +'.pdf'
                        src =  dest_folder + file
        
                        os.rename(src, dst)
        else: 
            print('No files to be moved to Archive.')
        return files
    except:
        print('Archive Failed')

  

def main():
######################## below are for production####################################################

 
    text=r"some file path removed for security"
    root_dir1=r"some file path removed for security"
    root_dir2=r"some file path removed for security"
    root_dir3=r"some file path removed for security"
    evolis_new_xlsx=r"some file path removed for security"
    pr4100_new_xlsx=r"some file path removed for security"
    dest_folder=r"some file path removed for security"

######################## below are for testing locally################################################

#######################################################################################################
    table_name ="removed for security"

    engine = sqlalchemy.create_engine('removed for security)
    
   
    filelist = lookin_folder(root_dir1,root_dir2)
    df_abid= extract_pdf(filelist) #dataframe for abid pdf's
    send_to_excel(df_abid, new_xlsx, text)
    send_to_sql(df_abid,table_name,engine,text)
    move_files(root_dir1, root_dir2, root_dir3, dest_folder) # check to see if files need to be archived
    messagebox.showinfo('Success!','PDF successfully uploaded to SQL!')
    
  
if __name__ == "__main__":

    main()# execute only if run as a script







