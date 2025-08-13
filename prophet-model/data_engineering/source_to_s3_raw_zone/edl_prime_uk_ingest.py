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
from pyspark.sql.types import LongType
from pyspark.sql.types import StructType,StructField, StringType,IntegerType
from pyspark.sql.functions import lit

s3 = boto3.resource('s3')
args = getResolvedOptions(sys.argv,['db_uk','cnctn_uk', 's3bucket'])

s3bucket = str(args['s3bucket'])
srcBucket = s3.Bucket(s3bucket)

tm = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]
ctm = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
schema = StructType([StructField('TotalCount' , IntegerType(), True),StructField('TableName' , StringType(), True)])

primetables = ['EVENT','LOSS_TYPE','LOSS_TYPE_QUALIFICATION', 'GROUP_ORGANISATION', 'COMPANY' , 'RESERVE_CATEGORY', 'EMPLOYEE' , 'EXCHANGE_RATE', 'INVOICE_ITEM' , 'MASTER_INVOICE', 'vw_assignment_owner', 'SLA', 'PARTY', 'ADHOC_PARTY' , 'RESERVE_TRANSACTION', 'PARTY_ASSIGNMENT_ROLE', 'APPORTIONMENT_SCHEDULE', 'INVOICE',  'EVENT_CANCELLED', 'WORK_IN_PROGRESS', 'TIME_PERIOD', 'WIP_TYPE', 'WIP_SCHEDULE', 'ASSIGNMENT', 'ACCOUNTING_CENTRE', 'BRANCH', 'LOSS_LOCATION', 'CATASTROPHE', 'APPLICATION_PARAMETERS', 'LOSS_ITEM_CATEGORY', 'BUSINESS_TYPE','INVOICE_APPORTIONMENT','csmMapping','refGLLDBusinessType', 'refGLLDLossType', 'CompanyIdentifier' , 'refGLLDReserveItem', 'csmDefinition','GDPRAssignmentsList','Dom_Com_Category_Type','PRICE_BASIS','ASSIGNMENT_TEAM','SCHEME']
#primetables = ['ASSIGNMENT_TEAM']

ss_tables = ['PARTY', 'ADHOC_PARTY' ,'COMPANY' ,'INVOICE_APPORTIONMENT','Dom_Com_Category_Type','PRICE_BASIS','WIP_TYPE','vw_assignment_owner','GDPRAssignmentsList', 'LOSS_TYPE_QUALIFICATION','GROUP_ORGANISATION','TIME_PERIOD', 'EVENT_CANCELLED','csmMapping','refGLLDBusinessType', 'refGLLDLossType', 'CompanyIdentifier' , 'refGLLDReserveItem', 'csmDefinition']
#ss_tables = ['ASSIGNMENT_TEAM']
co_prime_tables = ['csmMapping','refGLLDBusinessType', 'refGLLDLossType', 'CompanyIdentifier' , 'refGLLDReserveItem', 'csmDefinition']
#co_prime_tables = []
dictColumns = {'RESERVE_CATEGORY': ['Reserve_category_id','Last_updated_time'],'EXCHANGE_RATE':['Exchange_rate_id','Last_updated_time'],'INVOICE_ITEM':['Invoice_item_id','Last_updated_time'],'INVOICE':['Invoice_id','Last_updated_time'],'WORK_IN_PROGRESS':['Work_in_progress_id','Last_updated_time'],'APPORTIONMENT_SCHEDULE':['apportionment_schedule_id','Last_updated_time'],'MASTER_INVOICE':['Master_invoice_id','Last_updated_time'],'ASSIGNMENT':['Assignment_id','Last_updated_time'],'PARTY_ASSIGNMENT_ROLE':['Party_assignment_role_id','Last_updated_time'],'PARTY':['Party_id','Last_updated_time'],'COMPANY':['Company_id','Last_updated_time'],'EMPLOYEE':['Employee_code','Last_updated_time'],'EVENT':['Event_id','Last_updated_time'],'RESERVE_TRANSACTION':['Reserve_transaction_id','Last_updated_time'],'ACCOUNTING_CENTRE':['Accounting_centre_id','Last_updated_time'],'BRANCH':['Branch_code','Last_updated_time'],'LOSS_LOCATION':['Loss_location_id','Last_updated_time'],'CATASTROPHE':['Catastrophe_id','Last_updated_time'],'LOSS_TYPE':['code','Last_updated_time'], 'SLA':['sla_id','Last_updated_time'],  'ADHOC_PARTY':['Adhoc_party_id','Last_updated_time'], 'WIP_SCHEDULE':['wip_schedule_id','Last_updated_time'], 'APPLICATION_PARAMETERS':['last_updated_id','Last_updated_time'],  'LOSS_ITEM_CATEGORY':['code','Last_updated_time'],  'BUSINESS_TYPE':['code','Last_updated_time'], 'csmMapping':['MappingId','lastupdatedOn'], 'refGLLDBusinessType':['businesstypeid','lastupdatedon'],  'refGLLDLossType':['losstypeid','lastupdatedon'], 'CompanyIdentifier':['CompanyIdentifierID','LastUpdatedDate'], 'refGLLDReserveItem':['reserveitemid','lastupdatedon'],  'csmDefinition':['definitionid','lastupdatedon'],  'ASSIGNMENT_TEAM':['Assignment_team_id','Last_updated_time'],  'SCHEME':['Scheme_id','Last_updated_time'] }

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set('spark.executor.memoryOverhead', '15g')
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
print("****************Start fetching existinng connection info*******************")

dbDict = {args['cnctn_uk']: 'db_uk'}
cnList=['EDL-Prime-UK']

for cn in cnList:
    connection=glueContext.extract_jdbc_conf(str(cn))
    jdbcUrl = connection['url']+";database="+str(args[dbDict[str(cn)]])
    
    connectionProperties = {
      "user" : str(connection['user']),
      "password" : str(connection['password']),
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    print("****************Finished fetching existinng connection info*******************")
    pr,pr1 = dbDict[str(cn)].split('_')
    bucket_prefix = "flagfile/prime-uk/"
    objs = srcBucket.objects.filter(Prefix = bucket_prefix)
    print(objs)
    for ob in objs:
        print(ob.key)
        if(".txt" in ob.key):
            ptm,ext = ob.key.split('.txt')
    f,t0,t = ptm.split('/')
    print(str(t)+"***************Old time")
    print(str(ctm)+"***************Now time")
    finaldf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    for tbl in primetables:
        if(str(tbl) not in ss_tables):
            print("**************Table name : "+ str(tbl))
            #pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+" where "+str(dictColumns[str(tbl)][1])+"> '2021-02-15 01:34:00' and "+str(dictColumns[str(tbl)][1])+"<='"+str(ctm)+"' ) emp_alias"
            pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+" where "+str(dictColumns[str(tbl)][1])+">'"+str(t)+"') emp_alias"
            count_query = "(select count(*) as TotalCount from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+" ) emp_alias"
            print(pushdown_query)
            print("************Start fetching data from source************")
            dfnew = spark.read.option("numPartitions", 10).jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
            totalCountDf = spark.read.option("numPartitions", 10).jdbc(url=jdbcUrl, table=count_query, properties=connectionProperties)
            dfnew.coalesce(1).write.mode("append").parquet("s3://" + s3bucket + "/prime-"+str(pr1)+"/"+"dbo"+"/"+str(tbl)+"/")
            print("**********Table Finished : "+str(tbl))
            totalCountDf1= totalCountDf.withColumn("TableName" , lit(str(tbl)))
            finaldf = finaldf.union(totalCountDf1)
            spark.catalog.clearCache()
        else:
            current_time1 = datetime.now()
            print("**************Snap shot Table name : "+ str(tbl))
            if(str(tbl) in co_prime_tables):
                print("**************Co Prime Table name : "+ str(tbl))
                if(str(tbl) is "CompanyIdentifier"):
                    pushdown_query = "(select * from CrawfordClaims.dbo."+str(tbl)+" ) emp_alias"
                else:
                    print("**************Table name  more : "+ str(tbl))
                    pushdown_query = "(select * from ClaimsSystemMapping.dbo."+str(tbl)+" ) emp_alias"
                dfnew1 = spark.read.option("numPartitions", 10).jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties) 
                dfnew1.coalesce(1).write.mode("append").parquet("s3://" + s3bucket + "/prime-uk-co-db"+"/"+str(tbl)+"/")
                prefx = "prime-uk-co-db/"+str(tbl)+"/"
                print("**********Table Finished : "+str(tbl))
            else:
                pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+") emp_alias"
                dfnew = spark.read.option("numPartitions",10).jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
                print("Start writing file for snapshot************")
                dfnew.coalesce(1).write.mode("append").parquet("s3://" + s3bucket + "/prime-"+str(pr1)+"/"+"dbo"+"/"+str(tbl)+"/")
                print("**********Table Finished : "+str(tbl))
                prefx = "prime-"+str(pr1)+"/"+"dbo"+"/"+str(tbl)+"/"
            print("**********************"+prefx)

            for obj in srcBucket.objects.filter(Prefix = prefx):
                modified = obj.last_modified
                modified1 = datetime.strftime(modified,'%Y-%m-%d %H:%M:%S.%f')
                if modified1 < str(current_time1):
                    print("Deleting the old file from stage location")
                    obj.delete()
            spark.catalog.clearCache()
    finaldf.coalesce(1).write.mode("overwrite").parquet("s3://" + s3bucket + "/prime-"+str(pr1)+"/dbo/Totalcount/")        


if(".txt" in ob.key):
    ob.delete()
some_binary_data = b''
object = s3.Object(s3bucket, "flagfile/prime-uk/"+str(ctm)+".txt")
object.put(Body=some_binary_data)