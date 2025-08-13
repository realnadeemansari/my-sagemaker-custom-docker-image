import sys
import boto3
import pyspark
from boto3 import client
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import current_date
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, desc
import pyspark.sql.functions as F

args = getResolvedOptions(sys.argv,['db_name','db_au','cnctn_Navision','cnctn_AU','uks3bucket','uss3bucket','cnctn_AU_Prod','db_au_prod'])
print(args)
uks3bucket = str(args['uks3bucket'])
uss3bucket = str(args['uss3bucket'])
s3 = boto3.resource('s3')
srcBucket = s3.Bucket(uks3bucket)

tm = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]
#---fetching starttime
ctm = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set('spark.executor.memoryOverhead', '15g')
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
#spark.conf.set('spark.executor.memory', '10g')


#---Database Dictonary
dbDict1 = {args['cnctn_Navision']: args['db_name'],args['cnctn_AU']: args['db_au'],args['cnctn_AU_Prod']:args['db_au_prod']}

#---Dictonary for DB name and table name
dbDict = {'Crawford UK': '_GB080_Allianz_Customer_Ledger' , 'Crawford DE':  '_DE Live_Allianz_Customer_Ledger' ,'Prod_Navision_v2013': '_AU_Allianz_Customer_Ledger'}
#'Dev_Navision_v2013':'_AU_Allianz_Customer_Ledger'}
print('dbDict:'+str(dbDict))

#---DB list
dblist = ['Crawford UK', 'Crawford DE','Prod_Navision_v2013']
#dblist = ['Crawford UK']
#dblist = ['Crawford UK', 'Crawford DE']
#'Dev_Navision_v2013']
#dblist = ['Crawford DE','Dev_Navision_v2013']
print('dblist:'+str(dblist))

#---Country dictonary
countryDict = {'Crawford DE':'ge','Prod_Navision_v2013':'au','Crawford UK':'uk'}
#'Dev_Navision_v2013':'au'}
print('countryDict:'+str(countryDict))

#---Connection list
cnList=['EDL-Navision-EU','EDL-Navision-AU-Prod']
#'EDL-Navision-AU']
print('cnList:'+str(cnList))

#---Connection dictonary
dbConnDict = {'Crawford UK':'EDL-Navision-EU','Crawford DE':'EDL-Navision-EU','Prod_Navision_v2013':'EDL-Navision-AU-Prod'}
#'Dev_Navision_v2013':'EDL-Navision-AU'}
print('dbConnDict:'+str(dbConnDict))

#---Rename column function
def rename_cols(df):
    for column in df.columns:
        new_column = column.replace(' ','_').replace('(','_').replace(')','').replace('\r','')  
        df = df.withColumnRenamed(column, new_column)
        #print('successfully renamed')
    return df

#---Fetch Connection details function
def get_connection(db_name):
    print('Connection details for DB:'+str(db_name))
    cn = str(dbConnDict[str(db_name)])
    connection=glueContext.extract_jdbc_conf(str(cn))
    jdbcUrl = connection['url']+";database="+str(dbDict1[str(cn)])
    print(jdbcUrl)
    
    connectionProperties = {
      "user" : str(connection['user']),
      "password" : str(connection['password']),
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbcUrl,connectionProperties

for db in dblist:
    try:
        print("Processing for db:"+str(db))
        tbl = dbDict[str(db)]
        print("Table Name:"+str(tbl))
        print("****************Start fetching connection info*******************")
        jdbcUrl,connectionProperties = get_connection(str(db))
        print(connectionProperties)
        
        pushdown_query = "(select * from [" +  str(db) + "].dbo.[" + str(tbl) + "]) ac"
        print(pushdown_query)
        print('Into Spark read')
        dfnew = spark.read.option("numPartitions",10).jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
        print(dfnew.count())
        print("************New Schema**************")
        sp_df = rename_cols(dfnew)
    
        current_time = datetime.now()
        tbl = str(tbl).strip('_')
    
        if str(db) == "Crawford UK":
            print('Into If part')
            prefx = "navision-uk/Allianz/"+str(tbl)+"/"
            sp_df.coalesce(1).write.mode("append").parquet("s3://"+ uks3bucket + "/navision-uk/Allianz/"+str(tbl)+"/")
        else:
            print('Into else part')
            #country = countryDict[str(db)]
            #prefx = "Allianz/"+str(country)+"/"+str(tbl)+"/"
            prefx = "Allianz/edl_global_allianz/"+str(tbl)+"/"
            print(prefx)
            srcBucket = s3.Bucket(uss3bucket)
            print(srcBucket)
            sp_df.coalesce(1).write.mode("overwrite").parquet("s3://"+ uss3bucket + "/" + str(prefx))
            print('Written')
     
        for obj in srcBucket.objects.filter(Prefix = prefx):
            modified = obj.last_modified
            modified1 = datetime.strftime(modified,'%Y-%m-%d %H:%M:%S.%f')
            if modified1 < str(current_time):
                print("Deleting the old file from stage location")
                obj.delete()

        print("**********Table Finished : "+str(tbl))
    except Exception  as e:
        print("Exception ocurred************* : "+str(e))
        
    spark.catalog.clearCache()

'''for cn in cnList:
    print('connection:'+str(cn))
    connection=glueContext.extract_jdbc_conf(str(cn))
    #print(str(dbDict1[str(cn)]))
    jdbcUrl = connection['url']+";database="+str(dbDict1[str(cn)])
    print(jdbcUrl)
    connectionProperties = {
      "user" : str(connection['user']),
      "password" : str(connection['password']),
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    print("****************Finished fetching existing connection info*******************")
    try:
        for db in dblist: 
            print('For db:'+str(db))
            tbl = dbDict[str(db)]
            print("**************Table name : "+ str(tbl))
            pushdown_query = "(select * from [" +  str(db) + "].dbo.[" + str(tbl) + "]) ac"
            print(pushdown_query)
            dfnew = spark.read.option("numPartitions",10).jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
            print("Start writing file for snapshot************")
            sp_df = rename_cols(dfnew) 
            print("************ new schema **************")
            
            current_time = datetime.now()
            tbl = str(tbl).strip('_')
            if str(db) == "Crawford UK":
                prefx = "navision-uk/Allianz/"+str(tbl)+"/"
                sp_df.coalesce(1).write.mode("append").parquet("s3://"+ uks3bucket + "/navision-uk/Allianz/"+str(tbl)+"/")
            else:
                country = countryDict[str(db)]
                prefx = "Allianz/"+str(country)+"/"+str(tbl)+"/"
                srcBucket = s3.Bucket(uss3bucket)
                sp_df.coalesce(1).write.mode("append").parquet("s3://"+ uss3bucket + "/" + str(prefx))
            
            print("**********************  "+prefx)
            for obj in srcBucket.objects.filter(Prefix = prefx):
                modified = obj.last_modified
                modified1 = datetime.strftime(modified,'%Y-%m-%d %H:%M:%S.%f')
                if modified1 < str(current_time):
                    print("Deleting the old file from stage location")
                    obj.delete()  
            
            print("**********Table Finished : "+str(tbl))
    except Exception  as e:
        print("Exception ocurred************* : "+str(e))       
    spark.catalog.clearCache()'''