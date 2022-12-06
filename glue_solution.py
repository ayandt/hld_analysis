import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col,lit,split,udf,to_date
import boto3


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET_NAME','INPUT_PATH'])
bucketname = args['BUCKET_NAME']
inputpath=args['INPUT_PATH']

inputGDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": [f"{inputpath}"]}, format="csv",format_options={"withHeader": True, "separator": "\t"})
inputDF=inputGDF.toDF()

purchaseDF=inputDF.filter("event_list='1'").select(["product_list","ip","date_time"])
searchDF=inputDF.filter("referrer not like '%www.esshopzilla.com%'").select(["referrer","ip","date_time"])

def get_total_amount(product_list)->int:
    prd_arr = product_list.split(',')
    amount = 0.0
    for prd in prd_arr:
        amount += float(prd.split(';')[3])
    return amount

udf_get_amount= udf(lambda x:get_total_amount(x))


def get_keyword(url):
    d= url.split('.com')[0]
    domain = d.split('.')[1]
    if domain == 'google':
        a= url.split('&q=')
        b= a[1].split('&')[0]
        keyword= b.replace('+',' ')
    elif domain == 'yahoo':
        a= url.split('p=')
        b= a[1].split('&')[0]
        keyword= b.replace('+',' ')
    
    elif domain == 'bing':
        a= url.split('q=')
        b= a[1].split('&')[0]
        keyword= b.replace('+',' ')
        
    else:
        keyword = 'unknown'
        
    return keyword

udf_get_keyword= udf(lambda x:get_keyword(x))

purchaseDF=purchaseDF.withColumn("amount",udf_get_amount(col("product_list")).cast("double")).drop("product_list")
searchDF= searchDF.withColumn("domain", split(split("referrer",'.com').getItem(0),'\.').getItem(1))
searchDF=searchDF.withColumn("keyword",udf_get_keyword(col("referrer"))).drop("referrer")
joinDF =purchaseDF.join(searchDF,(purchaseDF.ip == searchDF.ip) & \
                        ( to_date(purchaseDF.date_time)==to_date(searchDF.date_time)),\
                        'inner').drop("ip","date_time")
outputDF=joinDF.groupBy("domain","keyword").sum("amount").withColumnRenamed("sum(amount)", "revenue").orderBy(col("revenue").desc())



outputDF.coalesce(1).write.option("delimiter", "\t").mode("overwrite").option("header",True).csv(f"s3://{bucketname}/out/")


s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucket)
source = "out/part"
filename = str(date.today())+"_SearchKeywordPerformance.tab"
for obj in my_bucket.objects.filter(Prefix=source):
    source_filename = (obj.key).split('/')[-1]
    copy_source = {
        'Bucket': bucket,
        'Key': obj.key
    }
    s3.meta.client.copy(copy_source, bucket, filename)

job.commit()