import awswrangler as wr
import boto3
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,['bucketname', 'schema', 'table'])
bucket_name = str(args['bucketname'])
schema = str(args['schema'])
table = str(args['table'])
redshift_conn = 'EDL-Redshift-US'

countrys = {}
s3_client = boto3.client("s3")
#bucket_name = "allianzvbr-sm"
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="prophet-model/output/forecasts/")
files = response.get("Contents")
for file in files:
    if file['Key'].split('/')[-2] != '' and file['Key'].split('/')[-2] != 'forecasts' and file['Key'].split('/')[-1] != '':
        countrys[file['Key'].split('/')[-2]] = file['Key']
print("Countries and its files  ")
print(countrys)
for country in countrys:
    inputPath = "s3://{}/{}".format(bucket_name, countrys[country])
#   inputPath = "s3://allianzvbr-sm/" + countrys[country]
    print("inputPath   " + inputPath)
    df = wr.s3.read_csv(
        path=[inputPath], 
        parse_dates=["forecast_date", "Load_date"])
    con_redshift = wr.redshift.connect(redshift_conn)
    wr.redshift.to_sql(
        df=df, 
        con=con_redshift, 
        schema= schema ,
        table= table,
        mode="upsert",
        primary_keys=["forecast_date","Region","Load_date"])
con_redshift.close()