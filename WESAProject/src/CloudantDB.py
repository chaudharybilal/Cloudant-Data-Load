# !/usr/local/bin/python2.7
# -*- coding: utf-8 -*-
import pypyodbc
import csv
import json
#from os.path import join, dirname
import sys, gc, logging
# from ExtractTransformLoad import File

from cloudant.client import Cloudant

#################
# Cloudant main #
#################

cloudant_username = "4d6e468f-cf33-4811-b42a-bcdc8c6659aa-bluemix"
cloudant_password = "1d9cb4172eaf56fac73a4c623aed063df03fd2f3908a50d1b883b78a89434a83"
cloudant_dbName = "python_test3"
db_ip = "9.3.33.74"
db_port = "1433"
db_name = "OAP_SD"
db_table = "OAP_SD.dbo.CloudantTest"
cols = ["*"]
db_usr = "sa"
db_pas = "passw0rd!"






def CreateDB(client, databaseName):
    client.connect() 
    try:
               
        data = client[databaseName]
        if data.exists():
            print 'Database already exists'
        else:
            client.create_database(databaseName)
                    # create database
        
        return(True)
    except:
        
        client.create_database(databaseName)
        print 'Database created successfully'
        return(True)

def Disconnect(client):
    try:
        client.disconnect()     # Disconnect server
 
        logging.info('Cloudant session disconnected')   
         
        return(True)
    except Exception as e:
        logging.error('Cloudant: ' + str(e))
        return(False)



def dataFetch(ip,port,db,tbl,cols,uid,pwd,client,cdb):
    client.connect()
    databasePointer = client[cdb]
    connection_string = 'Driver={SQL Server};Server='+ip+';PORT:'+port+';Database='+db+';Uid='+uid+';Pwd='+pwd+';'
    connection = pypyodbc.connect(connection_string)
    SQL = 'select '+','.join(cols)+' from ' + tbl
    cur = connection.cursor()
    res = cur.execute(SQL)

    columnList = [tuple[0] for tuple in res.description]
    print columnList
    
    a_json = list()
    while 1:
        row = cur.fetchone()
        if not row:
            break
    
        try:
            
            job = '{'
            for num in range(0,len(row)):
                if num == len(row) - 1:
                    job = job + ' "' + columnList[num] + '": "' + str(row[num]) + '"'
                else:
                    job = job + ' "' + columnList[num] + '": "' + str(row[num]) + '",'
            job = job + '}'   
            
            #print job
            
            databasePointer.create_document(json.loads(job))
        except Exception as e:
                logging.error(str(e))
                
   
    cur.close()
    return a_json


def main(argv):
    try:
        client = Cloudant(cloudant_username, cloudant_password, account=cloudant_username)
       
        CreateDB(client,'python_test3')
        
        dataFetch(db_ip,db_port,db_name,db_table,cols,db_usr,db_pas,client,cloudant_dbName)
        
        Disconnect(client)      # disconnect session
        
    except IOError as e:
        logging.error('CLoudant: ' + str(e))
    except Exception as e:
        logging.error('CLoudant: ' + str(e))
    finally:
        gc.collect()

if  __name__ =='__main__':
    main(sys.argv[1:])
