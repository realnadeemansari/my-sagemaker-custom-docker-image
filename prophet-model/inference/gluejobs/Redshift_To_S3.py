from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
import logging
import awswrangler as wr
import sys
from awsglue.utils import getResolvedOptions


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv,['bucketname', 'foldername', 'url', 'user', 'password', 'redshiftTmpDir', 'databasename', 'tablename'])
bucket_name = str(args['bucketname'])
folder_Name = str(args['foldername'])
redshift_URL = str(args['url'])
redshift_user = str(args['user'])
redshift_password = str(args['password'])
redshiftTmpDir = str(args['redshiftTmpDir'])
DbName = str(args['databasename'])
TblName = str(args['tablename'])

logger.info("Start")

country_code_query ='''Select distinct country_code from ''' + "{}.{}".format(DbName, TblName) + ''' where trim(product_name) <> 'Liability' and payment_date is not null'''

country_code_connection_options = {
        "url": redshift_URL,
        "user": redshift_user,
        "password": redshift_password,
        "query": country_code_query,
        "redshiftTmpDir": redshiftTmpDir        }

logger.info("After country_code_connection_options")

country_code_df = glueContext.create_dynamic_frame_from_options("redshift", country_code_connection_options).toDF()

logger.info("country_code_df count " + str(country_code_df.count()))

country_code_ls = country_code_df.select('country_code').rdd.map(lambda x : x[0]).collect()

logger.info("country_code_ls " + str(country_code_ls))

for country in country_code_ls: 
    # Query based read
    query ='''Select
        document_num,
        customer_id,
        case
            when country_code = 'UK' then sell_to_name
            else customer_name
        end as customer_name,
        cast(accounting_year as integer) as accounting_year,
        cast(accounting_month as integer) as accounting_month,
        TO_DATE(accounting_date, 'DD-MM-YYYY') as accounting_date,
        TO_DATE(invoice_date, 'DD-MM-YYYY') as invoice_date,
        country_code,
        client_category,
        client_group,
        currency_code,
        claim_number,
        cast(invoice_amt as numeric(38, 6)) as invoice_amt,
        cast(payment_amt as numeric(38, 6)) as payment_amt,
        company,
        sell_to_customer_no,
        sell_to_name,
        document_type,
        TO_DATE(payment_date, 'DD-MM-YYYY') as payment_date,
        ABS(CAST(net_amount as numeric(38, 6))) as net_amount,
        ABS(CAST(vat_amount as numeric(38, 6))) as vat_amount,
        regexp_replace(
            case when country_code = 'AU'
                    then
                        case when trim(product_name) in ('Commercial - Retail', 'Commercial - Wholesale')
                                then 'Property Commercial'
                            when trim(product_name) = 'Domestic'
                                then 'Property Residential'
                            else trim(product_name)
                        end
                when country_code = 'UK'
                    then 'Property Commercial'
                when country_code = 'DE'
                    then trim(product_name)
                else trim(product_name)
            end
        , '\\s', '') as product_name,
        crawford_gsl,
        product_description,
        invoice,
        ABS(CAST(invoice_fee as numeric(38, 6))) as invoice_fee,
        ABS(CAST(invoice_cost as numeric(38, 6))) as invoice_cost,
        assessment_type,
        peril,
        TO_DATE(creation_date, 'DD-MM-YYYY') as creation_date,
        local_to_eur_exchange_rate
    from 
        ''' + "{}.{}".format(DbName, TblName) + ''' 
        where
        trim(product_name) <> 'Liability'
    and payment_date is not null
    and country_code = ''' + "'" + country + "'"
    
    connection_options = {
        "url": redshift_URL,
        "query": query,
        "user": redshift_user,
        "password": redshift_password,
        "redshiftTmpDir": redshiftTmpDir        }
    
    logger.info("After connection")
    
    df = glueContext.create_dynamic_frame_from_options("redshift", connection_options).toDF()
    
    logger.info("After DF" + str(df.count()))
    
    # LOAD: Writing unique records in parquet format
    output_s3_path = "s3://{}/{}/".format(bucket_name,folder_Name) + country
    df.coalesce(1).write.mode("overwrite").option("header","true").csv(output_s3_path)
    
    logger.info("File written to S3 for country " + country )