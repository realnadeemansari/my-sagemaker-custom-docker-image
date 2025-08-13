import sys
import boto3
import pyspark
import pyspark.sql.functions as psf
from pyspark.sql.types import *
from boto3 import client
from awsglue.transforms import *
from pyspark.sql import Window
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Window, DataFrame
from botocore.exceptions import ClientError
from pyspark.storagelevel import StorageLevel
import time
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv,['writebucket','hubbucket'])

hubbucket = str(args['hubbucket'])
writebucket = str(args['writebucket'])

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
tm = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]
ptm1 = datetime.today() - timedelta(days=1)
ptm = ptm1.strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]
srcBucket = s3.Bucket(hubbucket)
#srcBucket1 = s3.Bucket(writebucket)

ctm = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
print("Printing the current time********* : "+str(ctm))
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set('spark.executor.memoryOverhead', '20g')
spark.conf.set("yarn.nodemanager.vmem-check-enabled", "false")
spark.conf.set('spark.yarn.executor.memoryOverhead', '20g')
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
spark.conf.set("spark.sql.crossJoin.enabled", "true")

print('Working bucket: {}'.format(hubbucket))

athena_client = boto3.client(service_name='athena', region_name='us-east-1')

def run_query(client, query,cntry,db,outputlocation):
    try:
        #print("****** db name is  *********  " + str(db))
        #print(" ****** query is *****  " + str(query))
        response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={ 'Database': str(db)},
        ResultConfiguration={ 'OutputLocation': "s3://"+hubbucket+"/copytoredshift/" +str(outputlocation) + "/viewresult/" + str(cntry)+"/" }
        )
        return response
    except Exception  as e:
        print("Exception ocurred************* : "+str(e))
    
def validate_query(client, query_id):
    resp = ["FAILED", "SUCCEEDED", "CANCELLED"]
    response = client.get_query_execution(QueryExecutionId=query_id)
    while response["QueryExecution"]["Status"]["State"] not in resp:
        response = client.get_query_execution(QueryExecutionId=query_id)
    return response["QueryExecution"]["Status"]["State"]

def read(query,cntry,db, outputlocation):
    print('start query: {}\n'.format(query))
    qe = run_query(athena_client, query,cntry,db,outputlocation)
    print("****************Query executed **********************")
    qstate = validate_query(athena_client, qe["QueryExecutionId"])
    print('query state: {}\n'.format(qstate))
    file_name = "copytoredshift/" + str(outputlocation)+ "/viewresult/"+str(cntry)+"/{}.csv".format(qe["QueryExecutionId"])
    obj = s3_client.get_object(Bucket=hubbucket, Key=file_name)
 
current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-7]

#dbdict = {'ge':'edl_ceaser_eu'}
#dbdict = {'ge':'edl_global_allianz_vbr'}
#dbName = dbdict[str(cntry)]
dbName = 'edl_global_allianz_vbr'

#countries=['au','us']
countries = ['au','ge']
#countries = ['ge']
try:
    for cntry in countries:
        qr1= "select * from  "+str(dbName)+".allianz_vbr_vw_" + str(cntry)   
        
        
        print(" ***** db name *** " +str(dbName))
        outlocation = 'global_allianz_vbr'
        
        read(qr1,cntry,dbName,outlocation)
        
        pfix = "copytoredshift/" + str(outlocation) + "/viewresult/" +str(cntry) +"/"
    
        for obj in srcBucket.objects.filter(Prefix=pfix):
            modified = obj.last_modified
            modified1 = datetime.strftime(modified,'%Y-%m-%d %H:%M:%S.%f')
            if modified1 < str(current_timestamp):
                print("Deleting the old file from viewresult location: "+str(obj.key))
                if('.csv' in obj.key):
                    obj.delete()
            
        inputpath = "s3://" + hubbucket + "/copytoredshift/"+str(outlocation)+ "/viewresult/" + str(cntry)+"/*.csv"    
        sp_df = spark.read.format("csv").options(header="true").option('multiLine', True).option("quote","\"").option("escape","\"").load(inputpath)
        print("** total count after csv read ** " + str(sp_df.count()))
        sp_df = sp_df.withColumn("INVOICE_AMT", sp_df["INVOICE_AMT"].cast("double").alias('INVOICE_AMT')).withColumn("PAYMENT_AMT", sp_df["PAYMENT_AMT"].cast("double").alias('PAYMENT_AMT'))
        #sp_df.printSchema()
        #sp_df.show(1)
        sp_df.coalesce(1).write.mode("append").parquet("s3://" + hubbucket + "/global_allianz_vbr/c_code="+str(cntry)+"/")
        
        pfix1 =  str(outlocation) + "/c_code=" +str(cntry) +"/"
    
        for obj1 in srcBucket.objects.filter(Prefix=pfix1):
            modified0 = obj1.last_modified
            modified2 = datetime.strftime(modified0,'%Y-%m-%d %H:%M:%S.%f')
            if modified2 < str(current_timestamp):
                print("Deleting the old file from write location: "+str(obj1.key))
                obj1.delete()
except Exception  as e:
    print("Exception ocurred************* : "+str(e))

print("***********************Job Succeeded*****************************")    