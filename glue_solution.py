import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col,split,udf,to_date
from datetime import date
import boto3

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

#resolving job input parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET_NAME','INPUT_PATH'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
bucketname = args['BUCKET_NAME']
inputpath=args['INPUT_PATH']

#reading the file and stored into pyspark dataframe
inputGDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": [f"{inputpath}"]}, format="csv",format_options={"withHeader": True, "separator": "\t"})
inputDF=inputGDF.toDF()

#filterered the purchesed info when event list is 1
purchaseDF=inputDF.filter("event_list='1'").select(["product_list","ip","date_time"])

#filterered the search url of outside of retailer site 
searchDF=inputDF.filter("referrer not like '%www.esshopzilla.com%'").select(["referrer","ip","date_time"])


#custom fuction to extact the total amount from product list
def get_total_amount(product_list)->int:
    prd_arr = product_list.split(',')
    amount = 0.0
    for prd in prd_arr:
        amount += float(prd.split(';')[3])
    return amount

udf_get_amount= udf(lambda x:get_total_amount(x))


#custom fuction to extact the search keyword from the url
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

#adding amount to the dataframe
purchaseDF=purchaseDF.withColumn("amount",udf_get_amount(col("product_list")).cast("double")).drop("product_list")

#adding domain to the dataframe
searchDF= searchDF.withColumn("domain", split(split("referrer",'.com').getItem(0),'\.').getItem(1))
#adding keyword to the dataframe
searchDF=searchDF.withColumn("keyword",udf_get_keyword(col("referrer"))).drop("referrer")

#joining purchase and search dataframe
joinDF =purchaseDF.join(searchDF,(purchaseDF.ip == searchDF.ip) & \
                        ( to_date(purchaseDF.date_time)==to_date(searchDF.date_time)),\
                        'inner').drop("ip","date_time")

#output dataframe with all total revenue with domain and keyword with most revenue at first
outputDF=joinDF.groupBy("domain","keyword").sum("amount").withColumnRenamed("sum(amount)", "revenue").orderBy(col("revenue").desc())
outputDF.coalesce(1).write.option("delimiter", "\t").mode("overwrite").option("header",True).csv(f"s3://{bucketname}/out/")


#copying the outfile as new name format
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucketname)
source = "out/part"
filename = str(date.today())+"_SearchKeywordPerformance.tab"
for obj in my_bucket.objects.filter(Prefix=source):
    source_filename = (obj.key).split('/')[-1]
    copy_source = {
        'Bucket': bucketname,
        'Key': obj.key
    }
    s3.meta.client.copy(copy_source, bucketname, filename)


job.commit()
