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
from pyspark.sql.types import StructType,StructField, StringType,IntegerType
from pyspark.sql.functions import lit


args = getResolvedOptions(sys.argv,['db_us','db_au','cnctn_us','cnctn_au', 's3bucket'])
s3bucket = str(args['s3bucket'])

s3 = boto3.resource('s3')
dest = s3.Bucket(s3bucket)

#tm = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]
ctm = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-7]

cmstables = ['UserBranch','CauseofLossCode', 'CauseCode', 'CatastrophicLossCode', 'PerilCode', 'ClaimTypeCodeCauseofLossCode', 'CoverageCode', 'ClaimTypeCodeCoverageCode' ,'ClaimTypeCodeLossDescCode', 'GlobalCIS', 'InvoicePayment', 'Organization', 'Armaster', 'Branch', 'RegionCode', 'Claim', 'ClaimTypeCode', 'ClaimGLLDFields', 'Docket', '"User"', 'ReserveCode', 'TimeCode', 'Address', 'LineOfBusinessCode', 'LossDescCode', 'DisabilityBenefit', 'DisabilityCode', 'Reserve', 'ClaimHealthcare', 'Claimant', 'SourceCode', 'DocketUnbilled', 'ClaimCoverage', 'ClaimSubscription', 'ClaimVendor', 'Bordereau', 'ProductLiability', 'OutcomeCode', 'Invoice', 'InvoiceAdjustment', 'InvoiceAdjustmentCode', 'InvoiceDetail', 'ClaimAssist', 'Note', 'ClaimStatusLog', 'Company','BordereauCode', 'AccountCodeName', 'AccountCode',  'BordereauExtraField', 'BordereauFieldCode','BranchTypeCode','TemplateData'] 
#cmstables = ['Docket','TimeCode']
ss_tables = ['"User"','BranchTypeCode','ClaimGLLDFields','DocketUnbilled','Bordereau','InvoiceAdjustment','LFMSTrustTransaction','BordereauCode', 'AccountCodeName', 'AccountCode',  'BordereauExtraField', 'BordereauFieldCode','UserBranch', 'CauseofLossCode', 'CauseCode' ,'CatastrophicLossCode', 'PerilCode' ,'ClaimTypeCodeCauseofLossCode', 'CoverageCode', 'ClaimTypeCodeCoverageCode', 'ClaimTypeCodeLossDescCode',  'InvoicePayment' ,'Organization' ,'Armaster' ,'Branch' ,'RegionCode' , 'ClaimStatusLog' ,'Company','GlobalCIS','TimeCode']

dictColumns = {'Claim': ['ID','lastchanged'], 'ClaimTypeCode': ['Code','LastChangedDate'], 'ClaimGLLDFields': ['ClaimID','LastChangedDate'], 'LineOfBusinessCode': ['Code','LastChangedDate'],
 'LossDescCode': ['Code','LastChangedDate'], 'DisabilityBenefit': ['ClaimID','LastChangedDate'], 'DisabilityCode': ['Code','LastChangedDate'], 'Reserve': ['ID','LastChanged'],
 'ClaimHealthcare': ['ID','LastChangedDate'], 'Claimant': ['ID','LastChanged'], 'SourceCode': ['Code','LastChangedDate'], 'Docket': ['ID','LastChanged'], 'DocketUnbilled':
 ['ID','LastChanged'], 'ClaimAssist': ['ID','LastChangedDate'], 'ClaimCoverage': ['ID','LastChangedDate'], '"User"': ['ID','LastChangedDate'], 'Address': ['ID','LastChangedDate'],'ReserveCode': ['Code','LastChangedDate'], 'ClaimSubscription': ['ID','LastChangedDate'],
 'ClaimVendor': ['ID','LastChangedDate'], 'Bordereau': ['ClaimID','LastChanged'], 'ProductLiability': ['ID','LastChangedDate'],
 'OutcomeCode': ['Code','LastChangedDate'], 'Invoice': ['ID','LastChangedDate'], 'InvoiceAdjustment': ['ID','LastChangedDate'], 'InvoiceAdjustmentCode': ['Code','LastChangedDate'],
 'InvoiceDetail': ['ID','LastChangedDate'], 'Note': ['ID','LastModifiedDate'],'TemplateData': ['ID','LastChangedDate']}

schema = StructType([StructField('TotalCount' , IntegerType(), True),StructField('TableName' , StringType(), True)])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set('spark.executor.memoryOverhead', '15g')
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

print("****************Start fetching existinng connection info*******************")
#dictionary for databases
dbDict = { args['cnctn_us']: 'db_us', args['cnctn_au']: 'db_au'}

#cnList=['EDL-CMS-US' , 'EDL-CMS-AU']
cnList=['EDL-CMS-US']

#cnList=['EDL-CMS-US'] 

for cn in cnList:
    connection=glueContext.extract_jdbc_conf(str(cn))
    jdbcUrl = connection['url']+";database="+str(args[dbDict[str(cn)]])
    
    connectionProperties = {
      "user" : str(connection['user']),
      "password" : str(connection['password']),
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    #print(connectionProperties)
    print("****************Finished fetching existinng connection info*******************")
    
    pr,pr1 = dbDict[str(cn)].split('_')
    bucket_prefix = "flagfile/flagfile-"+str(pr1)+"/"
    objs = dest.objects.filter(Prefix = bucket_prefix)
    print(objs)
    for ob in objs:
        print("kryyyyyyyyy" + str(ob.key))
        if(".txt" in ob.key):
            ptm,ext = ob.key.split('.txt')
    f,t0,t = ptm.split('/')
    print(str(t)+"***************Old time")
    print(str(ctm)+"***************Now time")
    finaldf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    
    #Read Write Branchid GSL mapping table for Australia
    print(cn)
    if (cn == 'EDL-CMS-AU'):
        au_gsl_query = "(SELECT DISTINCT claim.BranchID, DivisionState.BranchID AS 'Division/State', ( CASE WHEN DivisionState.BranchID = 'FRS' THEN 'GTS' WHEN DivisionState.BranchID = 'Liability' THEN 'GTS' WHEN DivisionState.BranchID = 'Construction & Engineering' THEN 'GTS' WHEN DivisionState.BranchID = 'GTS' THEN 'GTS' WHEN DivisionState.BranchID = 'Commercial' THEN 'GTS' WHEN DivisionState.BranchID = 'Virtual' THEN 'CCS' WHEN DivisionState.BranchID = 'BVL' THEN 'Contractor Connection' WHEN DivisionState.BranchID = 'Head Office' THEN 'SS' WHEN DivisionState.BranchID = 'Human Resources' THEN 'SS' WHEN DivisionState.BranchID = 'IT' THEN 'SS' WHEN DivisionState.BranchID = 'Finance' THEN 'SS' WHEN DivisionState.BranchID = 'Executive' THEN 'SS'  WHEN DivisionState.BranchID = 'Business Transformation Strategy' THEN 'SS' WHEN DivisionState.BranchID = 'Contractor Connection' THEN 'Contractor Connection' WHEN DivisionState.BranchID = 'Building Consultancy' THEN 'CCS' WHEN DivisionState.BranchID = 'Robotic process automation' THEN 'CCS' WHEN DivisionState.BranchID = 'Fusion' THEN 'CCS' WHEN DivisionState.BranchID = 'Licensee' THEN 'CCS' WHEN DivisionState.BranchID = 'Marine' THEN 'GTS' WHEN DivisionState.BranchID = 'Motor' THEN 'CCS' WHEN DivisionState.BranchID = 'GBSC' THEN 'CCS' WHEN DivisionState.BranchID = 'New South Wales' THEN 'CCS' WHEN DivisionState.BranchID = 'Prize and Indemnity' THEN 'CCS' WHEN DivisionState.BranchID = 'Queensland' THEN 'CCS' WHEN DivisionState.BranchID = 'South Australia' THEN 'CCS' WHEN DivisionState.BranchID = 'Tasmania' THEN 'CCS' WHEN DivisionState.BranchID = 'Victoria' THEN 'CCS' WHEN DivisionState.BranchID = 'STRATA' THEN 'CCS' WHEN DivisionState.BranchID = 'Western Australia' THEN 'CCS' WHEN DivisionState.BranchID = 'Other' THEN claim.BranchID ELSE DivisionState.BranchID END ) AS 'GSL', (SELECT COUNT(c.BranchID) FROM CMSAUReportLive.dbo.claim as c where c.BranchID = claim.BranchID GROUP BY c.BranchID) AS Frequency FROM claim OUTER APPLY (VALUES((CASE WHEN claim.BranchID IN ('25CAT','SY2X','FS2A','BL2A','CC2A','CN1A','COFS2A','DB2A','NR2A','CH2H','NW2A','SY2A','2CAT','TM2A','WG2A','12BAL','13BAL','46CAT','ULL2A','14CAT','15CAT','4CAT','5CAT','12CAT','ALLCC2A','SY2T','35CAT','42CAT','39CAT','40CAT','FS2A','19CAT','22CAT','SY2H') THEN 'New South Wales' WHEN claim.BranchID IN ('34CAT','BR4A','10TWE','CA4A','GC4A','MACK4A','BR4H','MR4A','WW4E','RH4A','10MKY','SC4A','TB4A','TW4A','3CAT','7CAT','10CAT','13CAT','17CAT','45CAT','43CAT', '23CAT','24CAT','48CAT','38CAT','36CAT','37CAT','32CAT','27CAT','BR4X','47CAT') THEN 'Queensland' WHEN claim.BranchID IN ('AD5X','AD5A','AD5F','DW5E','MG5A','NR5A','MO5A','PL5A','MD5A','CL5A','RN5A','44CAT','21CAT') THEN 'South Australia'  WHEN claim.BranchID IN ('HB7E','HB7A','LA7E','BU7A','BU7E','CASE1','33CAT') THEN 'Tasmania'  WHEN claim.BranchID  IN ('ML3X','11CAT','AW3C','BL3C','BN3C','GL3C','ML3A','MD3A','SH3C','ML3H','QBECC3A','WN3C','CG3H','WN3A','WR3A','1CAT','6CAT','1CAT','16CAT','16REG','8CAT','20CAT','28CAT','29CAT','26CAT') THEN 'Victoria' WHEN claim.BranchID IN ('9CAT','AL6A','BB6A','GR6A','GR6E','MB6E','PR6A','PR6H','PR6X','EP6A','18CAT','KB6A','41CAT','31CAT','DW0A') THEN 'Western Australia' WHEN claim.BranchID IN ('MG5CAR','CAR5A','CAR3A','MOT2A','46MOT','MOT5A','SY2M') THEN 'Motor' WHEN claim.BranchID IN ('FFAT1', 'FFAT2', 'FFAT4') THEN 'FRS' WHEN claim.BranchID IN ('COO', 'CEO', 'MGT') THEN 'Executive' WHEN claim.BranchID IN ('CONENG', 'CONEXP') THEN 'Construction & Engineering' WHEN claim.BranchID IN ('TPA1', 'TPA2') THEN 'TPA' WHEN claim.BranchID IN ('IT', 'ITDEV') THEN 'IT' WHEN claim.BranchID = 'DRONE' THEN 'Contractor Connection' WHEN claim.BranchID = 'WG2M' THEN 'Marine' WHEN claim.BranchID = 'GBSC' THEN 'GBSC' WHEN claim.BranchID = 'PRIZE' THEN 'Prize and Indemnity' WHEN claim.BranchID = 'RPA2' THEN 'Robotic process automation' WHEN claim.BranchID = '0000' THEN 'Head office' WHEN claim.BranchID = 'BTS' THEN 'Business Transformation Strategy' WHEN claim.BranchID = 'FIN' THEN 'Finance' WHEN claim.BranchID = 'HR' THEN 'Human Resources' WHEN claim.BranchID LIKE '%STRATA%' THEN 'STRATA' WHEN claim.BranchID LIKE '%MCKY' THEN 'Licensee' WHEN claim.BranchID LIKE '%RNET%' THEN 'Contractor Connection' WHEN claim.BranchID LIKE '%CFAS%' THEN 'GTS' WHEN claim.BranchID LIKE 'BVL%' THEN 'Building Validation' WHEN claim.BranchID LIKE 'CEG%' THEN 'Construction & Engineering' WHEN claim.BranchID LIKE 'COP%' THEN 'Code of Practice' WHEN claim.BranchID LIKE 'CPT%' THEN 'Complaints' WHEN claim.BranchID LIKE 'POW%' THEN 'Construction & Engineering' WHEN claim.BranchID LIKE 'TRI%' THEN 'Triage' WHEN claim.BranchID LIKE '%API%' THEN 'IT' WHEN claim.BranchID LIKE '%BLD%' THEN 'Building Consultancy' WHEN claim.BranchID LIKE '%VIR%' THEN 'Virtual' WHEN claim.BranchID LIKE '%BVL%' THEN 'Building Validation' WHEN claim.BranchID LIKE '%NSW%' THEN 'New South Wales' WHEN claim.BranchID LIKE '%QLD%' THEN 'Queensland' WHEN claim.BranchID LIKE '%TAS%' THEN 'Tasmania' WHEN claim.BranchID LIKE '%VIC%' THEN 'Victoria' WHEN claim.BranchID LIKE '%VIR%' THEN 'Virtual' WHEN claim.BranchID LIKE '%COM%' THEN 'Commercial' WHEN claim.BranchID LIKE '%MAR%' THEN 'Marine' WHEN claim.BranchID LIKE '%FRS%' THEN 'FRS' WHEN claim.BranchID LIKE '%VR%' THEN 'Virtual' WHEN claim.BranchID LIKE '%GT%' THEN 'GTS' WHEN claim.BranchID LIKE '%WA%' THEN 'Western Australia' WHEN claim.BranchID LIKE '%SA%' THEN 'South Australia' WHEN claim.BranchID LIKE 'CC%' THEN 'Contractor Connection' WHEN claim.BranchID LIKE '%RACQ%' THEN 'Queensland' WHEN claim.BranchID LIKE '%L' THEN 'Liability' WHEN claim.BranchID LIKE '%E' THEN 'Licensee' WHEN claim.BranchID LIKE '%F' THEN 'Fusion' ELSE 'Other' End ))) DivisionState(BranchID)) emp_alias1"
        AUgslDF = spark.read.option("numPartitions", 10).jdbc(url=jdbcUrl, table=au_gsl_query, properties=connectionProperties)
        AUgslDF.coalesce(1).write.mode("overwrite").parquet("s3://" + s3bucket + "/global_master/AU/au_gsl_data/")
    
    for tbl in cmstables:
        if(str(tbl) not in ss_tables):
            print("**************Delta Load Table name : "+ str(tbl))
            if(str(tbl)!='"User"'):
                pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+" where "+str(dictColumns[str(tbl)][1])+">'"+str(t)+"' and "+str(dictColumns[str(tbl)][1])+"<='"+str(ctm)+"' ) emp_alias"
                #pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+" where "+str(dictColumns[str(tbl)][1])+">'"+str(t)+"') emp_alias"
                #pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+" where "+str(dictColumns[str(tbl)][1]) + ">'2021-04-29 01:32:11') emp_alias"
                
                count_query = "(select count(*) as TotalCount from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+" where "+str(dictColumns[str(tbl)][1])+"<='"+str(ctm)+"' ) emp_alias"
            else:
                pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo.\"User\" where LastChangedDate>'"+str(t)+"' and LastChangedDate<='"+str(ctm)+"' ) emp_alias"
                count_query = "(select count(*) as TotalCount from "+str(args[dbDict[str(cn)]])+".dbo.\"User\" ) emp_alias"
            print(pushdown_query)
            print("************Start fetching data from source************")
            dfnew = spark.read.option("numPartitions", 10).jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
            
            totalCountDf = spark.read.option("numPartitions", 10).jdbc(url=jdbcUrl, table=count_query, properties=connectionProperties)
            if(str(tbl)=='"User"'):
                tbl='User'
            
            dfnew.coalesce(1).write.mode("append").parquet("s3://" + s3bucket + "/cms-"+str(pr1)+"/"+str(tbl)+"/")
            
            if(str(tbl) == 'Claim'):
                dfnew.coalesce(1).write.mode("append").parquet("s3://" + s3bucket + "/maskOut/CMS-"+str(pr1)+"-Mask Output/"+str(tbl)+"/")
                
            print("**********Delta Load Table Finished : "+str(tbl))
            
            totalCountDf1= totalCountDf.withColumn("TableName" , lit(str(tbl)))
            finaldf = finaldf.union(totalCountDf1)
            spark.catalog.clearCache()
        else:
            print("**************Snap Shot Table name : "+ str(tbl))
            pushdown_query = "(select * from "+str(args[dbDict[str(cn)]])+".dbo."+str(tbl)+") emp_alias"
            dfnew = spark.read.option("numPartitions",10).jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
            print("Start writing file for snapshot************")
            current_time1 = datetime.now()
            if(str(tbl)=='"User"'):
                tbl='User'
           
            dfnew.coalesce(1).write.mode("append").parquet("s3://" + s3bucket + "/cms-"+str(pr1)+"/"+str(tbl)+"/")
            print("**********Snap Shot Table Finished : "+str(tbl))
            prefx = "cms-"+str(pr1)+"/"+str(tbl)+"/"
            print("**********************"+prefx)
            
            for obj in dest.objects.filter(Prefix = prefx):
                modified = obj.last_modified
                modified1 = datetime.strftime(modified,'%Y-%m-%d %H:%M:%S.%f')
                if modified1 < str(current_time1):
                    print("Deleting the old file from stage location")
                    obj.delete()
            
            spark.catalog.clearCache()
    finaldf.show()
    
    finaldf.coalesce(1).write.mode("overwrite").parquet("s3://"+s3bucket + "/cms-"+str(pr1)+"/Totalcount/")
    print("write data into count table ")        
    #Deleting old flag file
    for ob in objs:
        print(ob.key)
    if(".txt" in ob.key):
        ob.delete()
        # logic for writing flag file for date
    some_binary_data = b''
    object = s3.Object(s3bucket, "flagfile/flagfile-"+str(pr1)+"/"+str(ctm)+".txt")
    object.put(Body=some_binary_data)