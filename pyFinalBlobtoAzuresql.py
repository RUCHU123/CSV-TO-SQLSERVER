# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 15:14:04 2019

@author: sankalp.patil
"""

import csv
import ast
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from azure.storage.blob import PublicAccess
import pyodbc
import os
import pandas as pd
from bcp import BCP, Connection, DataFile


#Blob

block_blob_service = BlockBlobService(account_name='samplsa', account_key='+M5icqu9BNzqTMfYMsYhFEROBjdgFHMIyYytsbBRqATVllUP0XyHcsgbxGmEC4zu0QtpW7rAn2Vf4PsBMVa5eg==')
container_name = 'targetcontainer'
block_blob_service.create_container(container_name)

# Set the permission so the blobs are public.
block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)


#Upload the CSV file to Azure storage account
local_path="C:\\Users\\sankalp.patil\\assignment\\upload"
for files in os.listdir(local_path):
    block_blob_service.create_blob_from_path(container_name,files,os.path.join(local_path,files))
    
server = 'sample-server1.database.windows.net'
database = 'targetdb'
username = 'sankalp'
password = '#themysticyogi1'
driver= '{ODBC Driver 13 for SQL Server}'


#database connection
def sqlconnect(server,database,username,password):
    try:
        return pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    except:
        print ("connection failed check authorization parameters")  
conn = sqlconnect(server,database,username,password)
cursor = conn.cursor()

# this is one time activity:

#sql = "create external data source j with(type = BLOB_STORAGE,location = 'https://samplsa.blob.core.windows.net/{0}' )"
#sql = sql.format(container_name)
#cursor.execute(sql)


for files in os.listdir(local_path):
    path=os.path.join(local_path,files)
    filename=(os.path.splitext(files)[0])
    tablename = 'dbo.'+ filename
    print(files)
    print(path)
    print(tablename)
    
    f = open(path, 'r')
    reader = csv.reader(f)

    longest, headers, type_list = [], [], []
        
    def dataType(val, current_type):
        try:
            # Evaluates numbers to an appropriate type, and strings an error
            t = ast.literal_eval(val)
        except ValueError:
            return 'varchar'
        except SyntaxError:
            return 'varchar'
        if type(t) in [int,float]:
            if (type(t) in [int]) and current_type not in ['float', 'varchar']:
                # Use smallest possible int type
                if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                    return 'smallint'
                elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                    return 'int'
                else:
                    return 'bigint'
            if type(t) is float and current_type not in ['varchar']:
                return 'decimal'
        else:
            return 'varchar'
    
    for row in reader:
        if len(headers) == 0:
            headers = row
            for col in row:
                longest.append(0)
                type_list.append('')
        else:
            for i in range(len(row)):
                # NA is the csv null value
                if type_list[i] == 'varchar' or row[i] == 'NA':
                    pass
                else:
                    var_type = dataType(row[i], type_list[i])
                    type_list[i] = var_type
                if len(row[i]) > longest[i]:
                    longest[i] = len(row[i])
    f.close()
    
    print(longest)
    print(headers)
    print(type_list)
    
    sql = "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id=OBJECT_ID(N'[dbo].[{0}]') and type in (N'U')) BEGIN CREATE TABLE {1} (".format(filename,tablename)     
    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            sql = (sql + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
        else:
            sql = (sql + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

    sql = sql[:-1] + ') END'
    print(sql)
    #sql=sql.format(tablename)
    cursor.execute(sql)
    
    sql = "truncate table {}".format(tablename)
    print(sql)
    cursor.execute(sql)
    sql = "BULK INSERT {0} FROM '{1}' WITH ( FIRSTROW=2, DATA_SOURCE = 'j',FIELDTERMINATOR=',', ROWTERMINATOR = '\\n')"
    sql = sql.format(tablename,files)
    print(sql)
    cursor.execute(sql)
    
conn.commit()
cursor.close()
print("Done")
conn.close()