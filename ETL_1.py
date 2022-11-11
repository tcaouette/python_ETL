

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
import tabula
import PyPDF2 as pypdf2 
import camelot
import math
import pyodbc
import time
import sqlalchemy
from sqlalchemy import create_engine
import urllib
from os import path
import glob
#######################################################################################################################################################
# 
#    Author: Tobias Caouette 
#    Date: June 29th, 2020
#    Revision: 1
#    Description: 
#                    The dataframes are passed to functions that create excel documents and functions that send the dataframes to SQL.
#                   Once completed the files are moved to an archival location as well as being renamed with datetime stamps.
#######################################################################################################################################################

def lookin_folder(root_dir1,root_dir2, r_exfile=None):# will have to account for another root_dir
    '''Looks in a folder via a path and builds a list with files that have extension pdf'''
    evolis_files =[]
    pr41_files =[]
    evolis_filelist=[]
    pr41_filelist=[]
   
    if len(listdir(root_dir1)) >= 1:
        for file in listdir(root_dir1):
            if file.endswith('.txt'):
                evolis_files.append(file)
                #ex_files = [i for i in ex_files if not i.startswith('~$')] # ----------- removes ~$ issue from list
                evolis_filelist = [root_dir1 + i for i in evolis_files]
    if len(listdir(root_dir2)) >= 1:
        for file in listdir(root_dir2):
            if file.endswith('.txt'):
                pr41_files.append(file)
                pr41_filelist = [root_dir2 + i for i in pr41_files]
    files = evolis_filelist + pr41_filelist
    return files
 

def evolis_add_meta(txt_file_paths):
    '''Builds dataframe from the  text file, if there are multiple text files, each dataframe is appended to one another. 
        If no text files sends a blank list as a dataframe to the sql and excel functions'''
    if len(txt_file_paths) > 0:
        append_evolis = pd.DataFrame()
        for path in txt_file_paths:
            if 'Evolis' in path:
                evolis_data = pd.read_csv(path,skiprows=23, sep="|",na_filter = True)
                evolis_data = evolis_data[:-1] # removes last row which is not apart of the main dataset
                rows_to_keep = [1,2,3,5]
                evolis_meta = pd.read_csv(path, skiprows = lambda x: x not in rows_to_keep,sep='|', header=None) #Grabs Meta Data in the 'Head of the File'
                transpose_eMeta = evolis_meta.T # Matrix transpose - reshapes the data 
                new_header = transpose_eMeta.iloc[0]  # resetting the header           
                transpose_eMeta = transpose_eMeta[1:] 
                transpose_eMeta.columns = new_header
                eMeta_repeated = pd.concat([transpose_eMeta]*len(evolis_data), ignore_index=True) # Repeating the meta data to the length of body of data set
                join_df = evolis_data.join(eMeta_repeated) # joining the two data frames together
                append_evolis = append_evolis.append(join_df) # appending each dataframe to gether to create one main dataframe to be passed to sql and excel
        return  append_evolis
    else:
        df =[]
        return df



def extract_pr4100(txt_file_paths):
    '''Builds dataframe from the  text file, if there are multiple text files, each dataframe is appended to one another. 
        If no text files sends a blank list as a dataframe to the sql and excel functions'''
    if len(txt_file_paths) > 0:

        append_pr41 = pd.DataFrame()
        for path in txt_file_paths:
            if 'PR4100' in path:
                pr41_data = pd.read_csv(path, skiprows=2, header=None, sep='|')
                #comments = pr41_data.loc[pr41_data[0] == 'C', :]
                pr41_data[2]=pr41_data[2].str.strip('^^^BRSARSt') # removing ^^^BRSARSt to clean the column for future needs
                pr41_data[6] = pr41_data[6].apply(lambda x: '{:.0f}'.format(x)) # reformatting the date columns to ensure that the date isn't displayed as scientific notation
                pr41_data = pr41_data.loc[(pr41_data[0] != 'P') & (pr41_data[0] != 'C') & (pr41_data[0] != 'L'), :] # remove rows starting with P,L,C.
                pr41_data[[7,8]] = pr41_data[2].str.split("^",expand=True) # splitting column 2 since this has P row data (P data isn't lost)
                pr41_data.drop([1,2,4,5,7], axis=1, inplace=True) # drop columns that do not have data or are redundant.
                append_pr41 = append_pr41.append(pr41_data) # appending each dataframe together to create one main dataframe to be passed to sql and excel
        return append_pr41
    else:
        df = []
        return df




def pr41_list_df(df): # df is short for dataframe (the dataframe is from extract_pr4100)
    '''Chunks the dataframe, 6 rows per chunk. these chunks are then appened in a list '''

    try:
        n = 6  #chunk row size
        df = [df[i:i+n] for i in range(0,df.shape[0],n)] # chunk the dataframe into N chunks in a long list of dataframes
        return df
    except:
        return df 



def pr41_move_add_row(df):# df is short for dataframe (the dataframe is from pr41_list_df)
    '''Setting specific locations in each dataframe in the list to another locations' value  '''
    try:
        for i in range(len(df)):
            df[i].iloc[0,1] = df[i].iloc[1,3] 
            df[i]=df[i].append(pd.Series(), ignore_index=True) # creating a new row for the move
        return df
    except:
        return df 
    

def pr41_move_date(df):#df is short for dataframe (the dataframe is from pr41_move_add_row)
    '''Setting each data frame in the list specified location to R this is for a new header row. Then Setting specific locations in each dataframe in the list to another locations' value '''
    try:
        for i in range(len(df)):
            df[i].iloc[6,0]='R' #adding R into specific location to mirror the 0 column
            df[i].iloc[6,1]=df[i].iloc[0,2] #finally move the date
        return df
    except:
        return df	



def pr41_drop_cols(df):#df is short for dataframe (the dataframe is from pr41_move_date)
    '''Dropping two columns that no longer have data since these columns values have been moved in the previous functions '''
    try:
        for i in range(len(df)):
            df[i].drop(6, axis=1, inplace=True) #dropping column 6
            df[i].drop(8, axis=1, inplace=True) #dropping column 8
        return df
    except:
        return df	


def transpose_pr41(df):#df is short for dataframe (the dataframe is from pr41_drop_cols)
    '''Simple matrix transpose for each of the dataframes in the list. This builds the final dataframe by appending each transposed dataframe to one another'''
    try:
        df_append=[]
        for i in range(len(df)):
            df_append.append(df[i].set_index(0).transpose())# matrix transpose on each dataframe in list
        transposed_pr41 = pd.concat(df_append) # appending each transposed matrix 
        transposed_pr41.columns=['Well','Sample ID','Layout','OD for Cutoff','S/CO','Cutoff Result','Date'] # setting the new column headers
        return transposed_pr41
    except:
        return df


def evolis_send_to_excel(df, new_xlsx, text):
    '''Builds the excel file from the evolis dataframe, a success and or fail message is written to a text document '''
    add_date = dt.datetime.now().strftime("%Y%m%d_%H:%M:%S")
    try:
        if df is not None:
            with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
                file_object.seek(0)
    # If file is not empty then append '\n'
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
    # Append text at the end of file
                yay = "Evolis Scrape Completed and Excel File Generated " + add_date
                file_object.write(yay)
        
                df.to_excel(new_xlsx, index=False)
        
            return print("Evolis Excel Doc Created")# need to add a header to the dataframe before it gets exported 
        else:
             with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
                file_object.seek(0)
    # If file is not empty then append '\n'
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
    # Append text at the end of file
                yay = "Evolis Excel File Not created No Evolis text files " + add_date
                file_object.write(yay)
    except:
        with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
            file_object.seek(0)
    # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
    # Append text at the end of file
            failed = "Evolis Scrape Failed and No Excel File Created " + add_date
            file_object.write(failed)

def pr41_send_to_excel(df, new_xlsx, text):
    '''Builds the excel file from the pr4100 dataframe, a success and or fail message is written to a text document '''
    add_date = dt.datetime.now().strftime("%Y%m%d_%H:%M:%S")
    try:
        if len(df.values.tolist()) > 0: 
            with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
                file_object.seek(0)
    # If file is not empty then append '\n'
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
    # Append text at the end of file
                yay = "PR4100 Scrape Completed and Excel File Generated " + add_date
                file_object.write(yay)
        
                df.to_excel(new_xlsx, index=False)
        
            return print("PR4100 Excel Doc Created")# need to add a header to the dataframe before it gets exported 
        else:
            with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
                file_object.seek(0)
    # If file is not empty then append '\n'
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
    # Append text at the end of file
                yay = "PR4100 Excel File Not created No Evolis text files  " + add_date
                file_object.write(yay)
    except:
        with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
            file_object.seek(0)
    # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
    # Append text at the end of file
            failed = "PR4100 Scrape Failed and No Excel File Created " + add_date
            file_object.write(failed)

def evolis_send_to_sql(df,table_name,engine,text):# customize for evolis and pr4100
    '''Send the evolis dataframe to the sql server and database. A success and or fail message is written to a text document '''
    add_date = dt.datetime.now().strftime("%Y%m%d_%H:%M:%S")
    
    if len(df)>0:
        
        df['dateinsert'] = pd.to_datetime('today')
        df.to_sql(table_name,engine,if_exists='replace',index=False,dtype={'Patient ID':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Assay':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Well':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Flag':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'OD':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'S/CO':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Result':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Instrument ID:':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Time:':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'DATE:':sqlalchemy.types.DATE(),
                                                                        'Operator:':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'dateinsert':sqlalchemy.types.Date(),
                                                                        
                                                                      })
        time.sleep(30)#pauseing for 30 seconds to ensure import has finished so archive can happen without error
        with open(text, "a+") as file_object:
        # Move read cursor to the start of file.
            file_object.seek(0)
        # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
        # Append text at the end of file
            excellent = "Evolis Data Successfully Appended to SQL Table " + add_date
            file_object.write(excellent)
    else:    
        df = pd.DataFrame([{'Patient ID':' ','Assay':' ','Well':' ','Flag':' ','OD':' ','S/CO':' ','Result':' ','Instrument ID:':' ','Time:':' ','DATE:':' ','Operator:':' ','dateinsert':' '}])

        df.to_sql(table_name,engine,if_exists='replace',index=False)
        with open(text, "a+") as file_object:
        # Move read cursor to the start of file.
            file_object.seek(0)
        # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
        # Append text at the end of file
            excellent = "Evolis Dataframe Not Availible for Upload to Sql " + add_date
            file_object.write(excellent)
       # print("Check Database Connection")

def pr41_send_to_sql(df,table_name,engine,text):# customize for evolis and pr4100
    '''Send the pr4100 dataframe to the sql server and database. A success and or fail message is written to a text document '''
    add_date = dt.datetime.now().strftime("%Y%m%d_%H:%M:%S")

    if len(df)> 0:

        df['dateinsert'] = pd.to_datetime('today')
        df.to_sql(table_name,engine,if_exists='replace',index=False,dtype={'Well':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Sample ID':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Layout':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'OD for Cutoff':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'S/CO':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Cutoff results':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'Date':sqlalchemy.types.NVARCHAR(length=50),
                                                                        'dateinsert':sqlalchemy.types.Date(),
                                                                      })
        time.sleep(30)#pauseing for 30 seconds to ensure import has finished so archive can happen without error
        with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
            file_object.seek(0)
    # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
    # Append text at the end of file
            excellent = "PR4100 Data Successfully Appended to SQL Table " + add_date
            file_object.write(excellent)
    else:
        df = pd.DataFrame([{'Well':' ','Sample ID':' ','Layout':' ','OD for Cutoff':' ','S/CO':' ','S/CO':' ','Cutoff result':' ','Date':' ','dateinsert':' '}])

        df.to_sql(table_name,engine,if_exists='replace',index=False)
        with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
            file_object.seek(0)
    # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
    # Append text at the end of file
            excellent = "PR4100 Dataframe Not Availible for Upload to Sql " + add_date
            file_object.write(excellent)

# move excel file before writing new excel file and move pdf's to new folder
def move_files(root_dir1, root_dir2,root_dir3, dest_folder,text):
    '''Moves files to the archive(dest_folder). root_dir1 is the directory for  and root_dir1 directory for , root_dir3 is for the excel files,dest_folder is archive for both txt and excel files'''
    txt_evolis_files =[]
    txt_pr41_files=[]
    excel_file=[]
    root_files1=[]
    root_files2=[]
    root_files3=[]
    #all_root_dirs = []
    add_date = dt.datetime.now().strftime('%Y%m%d_%H%M%S%f')
    
    if len(listdir(root_dir1)) >= 1:
        for file in listdir(root_dir1):
            if file.endswith('.txt'):
                txt_evolis_files.append(file)
                root_files1 = [root_dir1 + i for i in txt_evolis_files]
                shutil.move(os.path.join(root_dir1, file), dest_folder)

                
    if len(listdir(root_dir2)) >= 1:
        for file in listdir(root_dir2):
            if file.endswith('.txt'):
                txt_pr41_files.append(file)
                root_files2 = [root_dir2 + i for i in txt_pr41_files]
                shutil.move(os.path.join(root_dir2, file), dest_folder)

    if len(listdir(root_dir3)) >= 1:
        for file in listdir(root_dir3):
            if file.endswith('.xlsx'):
                excel_file.append(file)
                root_files3 = [root_dir3 + i for i in excel_file]
                shutil.move(os.path.join(root_dir3, file), dest_folder)
                
                
    files = root_files3  + root_files2 + root_files3
    print(files)
    try:
        if len(files) > 0:
    
            for file in listdir(dest_folder):
                if file.endswith('.xlsx'):# if file ends with .txt or xlsx, then remove the .xlsx and .txt from name then create new name append the xlsx and txt back
                    if not file.startswith("arc"):
                        dst = dest_folder + 'arc_'+   file[:-5] +'_' +  add_date +'.xlsx'
                        src =  dest_folder + file
                        #shutil.move(src, dst)
                        os.rename(src, dst)
                if file.endswith('.txt'):
                    if not file.startswith("arc"):
                        dst = dest_folder + 'arc_'+   file[:-4] +'_' +  add_date +'.txt'
                        src =  dest_folder + file
                        #shutil.move(src,dst)
                        os.rename(src, dst)
            print('Archive Successful')
        else: 
            print('No files to be moved to Archive.')
        #return files
    except:
        with open(text, "a+") as file_object:
    # Move read cursor to the start of file.
            file_object.seek(0)
    # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0 :
                file_object.write("\n")
    # Append text at the end of file
            excellent = "Archive Failed " + add_date
            file_object.write(excellent)


def main():
    '''Calls all functions above in sequential order. Where global variables are defined'''
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
    evolis_table_name ="removed for security"
    pr41_table_name ="removed for security"

    engine = sqlalchemy.create_engine('info removed for security)
    
   
    txt_file_paths = lookin_folder(root_dir1,root_dir2)
    ##evolis
    
    evolis_df = evolis_add_meta(txt_file_paths)
        
    evolis_send_to_excel(evolis_df, evolis_new_xlsx, text)
    evolis_send_to_sql(evolis_df,evolis_table_name,engine,text)
    print('Evolis dataframe sent to SQL')
    ##PR41
        
    pr41_data = extract_pr4100(txt_file_paths)
    
    list_df = pr41_list_df(pr41_data)
    
    new_df = pr41_move_add_row(list_df)
    
    df_R = pr41_move_date(new_df)
    
    cleaned_pr41 = pr41_drop_cols(df_R)
    
    transposed_pr41 = transpose_pr41(cleaned_pr41)
    
    pr41_send_to_excel(transposed_pr41,pr4100_new_xlsx , text)
    
    
    pr41_send_to_sql(transposed_pr41,pr41_table_name,engine,text)
    print(' dataframe sent to SQL')
            ###end pr41

        
    move_files(root_dir1, root_dir2, root_dir3, dest_folder,text)

    
if __name__ == "__main__":
    # execute only if run as a script
    main()






