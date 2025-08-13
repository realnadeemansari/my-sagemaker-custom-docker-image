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
from pyspark.sql.functions import current_date,udf,coalesce
from pyspark.sql import SparkSession, Window, DataFrame
from botocore.exceptions import ClientError
from pyspark.storagelevel import StorageLevel
import time
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv,['hubbucket', 'rawbucket'])

hubBucket = str(args['hubbucket'])
rawBucket = str(args['rawbucket'])

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
tm = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]
ptm1 = datetime.today() - timedelta(days=1)
ptm = ptm1.strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]

srcBucket = s3.Bucket(rawBucket)

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

print('Working bucket: {}'.format(hubBucket))
athena_client = boto3.client(service_name='athena', region_name='us-east-1')
#s3://crawco.clm.raw.edl.dev/Allianz/au/Exhanges_Rate_Historical_Aud_Eur/
def run_query(client, query,cntry,db):
    try:
        pfx='load'
        response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={ 'Database': str(db)},
        ResultConfiguration={ 'OutputLocation': "s3://"+rawBucket+"/Allianz/" +str(cntry)+"/Exhanges_Rate_Historical_Aud_Eur/" }
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

def read(query,cntry,db):
    print('start query: {}\n'.format(query))
    qe = run_query(athena_client, query,cntry,db)
    print("****************Query executed **********************")
    qstate = validate_query(athena_client, qe["QueryExecutionId"])
    print('query state: {}\n'.format(qstate))
    file_name = "Allianz/" +str(cntry)+ "/Exhanges_Rate_Historical_Aud_Eur/{}.csv".format(qe["QueryExecutionId"])
    obj = s3_client.get_object(Bucket=rawBucket, Key=file_name)

current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-7]
dbdict = {'au':['edl_global_allianz_vbr','EUR','Euro']}
countries=['au']
#countries=['it']
try:
    for cntry in countries:
        #qr1="select distinct docid,seq,creationdate,filerefnumber,handlingbranch,globalregion,handlingcountry,adjuster,adjusteremail,insuredname,lossaddress1,lossaddress2,losscity,lossstate,losszip,losscountry,leadinsurer,insurerlocation,insurercontactname,insurerinterest,insurerclaimnumber,broker,dateofloss,perillosstype,totalreservelocalcurrency,totalreserve,finalsettlementlocalcurrency,finalsettlement,feeslocalcurrency,fees,dateassigned,productline,industrysector,subsector,claimstatus,supervisor,coinsurer1,interest1,coinsurer2,interest2,coinsurer3,interest3,coinsurer4,interest4,coinsurer5,interest5,coinsurer6,interest6,coinsurer7,interest7,coinsurer8,interest8,coinsurer9,interest9,coinsurer10,interest10,coinsurer11,interest11,coinsurer12,interest12,lossitem1,lossreserve1localcurrency,lossreserve1,lossitem2,lossreserve2localcurrency,lossreserve2,lossitem3,lossreserve3localcurrency,lossreserve3,lossitem4,lossreserve4localcurrency,lossreserve4,lossitem5,lossreserve5localcurrency,lossreserve5,lossitem6,lossreserve6localcurrency,lossreserve6,lossitem7,lossreserve7localcurrency,lossreserve7,lossitem8,lossreserve8localcurrency,lossreserve8,lossitem9,lossreserve9localcurrency,lossreserve9,lossitem10,lossreserve10localcurrency,lossreserve10,comments,commentsfin,insurerparent,totalincurredlocalcurrency,totalincurredusdollar,expensepaymentlocalcurrency,expensepaymentusdollar,indemnitypaymentlocalcurrency,indemnitypaymentusdollar,adjuster1,adjuster2,adjuster3,adjuster4,adjuster5,lastmodifieddate,origtotalincurlocalcurrency,origtotalincur,0 as highesttotalincurlocalcurrency,0 as highesttotalincur,policynumber,SourceOfClaimNotification,reserveentered,reserveincreased,amt_reserve_increase,global_serviceline,businessvertical,currentWIP,CATOrBAU_claim,CurrentCloseDate,CurrentReopenDate,GrossReserve,OriginalCloseDate,OriginalReopenDate,NetReserve,Totalindemnitypayment,author,currencyrate,currency,isCmslargeloss  from gld_"+str(cntry)+"_vw_final"
        qr1="select * from  (select date_format(now() - interval '1' day,'%m/%d/%Y') as effdt,a.au_usd as aud_usd_div,b.usd_eur as usd_eur_mul from (SELECT effdt,rate_div as au_usd FROM edl_peoplesoft_us.ps_rt_rate_tbl where trim(from_cur)='AUD' and trim(to_cur)='USD' and effdt = now() - interval '1' day ) a left join (SELECT effdt,rate_mult as usd_eur FROM edl_peoplesoft_us.ps_rt_rate_tbl where trim(from_cur)='USD' and trim(to_cur)='EUR' and effdt = now() - interval '1' day) b on (a.effdt = b.effdt) union (SELECT  date_format(now() - interval '1' day,'%m/%d/%Y') as effdt ,cast(aud_usd_div as decimal(15,8)) as aud_usd_div,cast(usd_eur_mul as decimal(15,8)) as usd_eur_mul FROM au_historical_exchange_rates  order by date_parse(effdt,'%m/%d/%Y') desc limit 1)) limit 1"
        current_timestamp = datetime.now()
        dbName = dbdict[str(cntry)][0]
        currencyCode = dbdict[str(cntry)][1]
        currencyName = dbdict[str(cntry)][2]
        print("**Data base name is-  " + str(dbName))
        #time_entries_df = read(qr1,cntry,dbName)
        read(qr1,cntry,dbName)
    pfix1 =  "Allianz/" +str(cntry)+"/Exhanges_Rate_Historical_Aud_Eur/"
    for obj1 in srcBucket.objects.filter(Prefix=pfix1):
        #print("I am in deletion")
        modified0 = obj1.last_modified
        modified2 = datetime.strftime(modified0,'%Y-%m-%d %H:%M:%S.%f')
        #print(obj1.key)
        if('metadata' in obj1.key):
            #if modified2 < str(current_timestamp):
            print("Deleting the metadat file from write location: "+str(obj1.key))
            obj1.delete()
        
except Exception  as e:
    print("Exception ocurred************* : "+str(e))  
    
print("***********************Job Succeeded*****************************")