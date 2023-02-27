import os
import io
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import boto3
import shutil
from zipfile import ZipFile


spark = SparkSession.builder.appName("name2").getOrCreate()

s3=boto3.client('s3')

bucket = 'gbatch-training-2023'
inbound = 'shubham/inbound/'
landing = 'shubham/landing/'
temp_dir = 'shubham/temp/'
pref = 'shubham/inbound/'
unzip_pref = 'shubham/temp/'

# Get a list from specified prefix
objects = s3.list_objects(
    Bucket=bucket,
    Prefix=pref
)["Contents"]

# Take the unzipped files 
unzipped_obj = s3.list_objects(
    Bucket=bucket,
    Prefix=unzip_pref
)["Contents"]

# Get a list containing the keys of the objects to unzip
object_keys = [ o["Key"] for o in objects if o["Key"].endswith(".zip") ] 
# Get the keys for the unzipped objects
unzipped_object_keys = [ o["Key"] for o in unzipped_obj ] 

for key in object_keys:
    obj = s3.get_object(
        Bucket= bucket,
        Key=key
    )
    
    objbuffer = io.BytesIO(obj["Body"].read())
    
    with ZipFile(objbuffer) as zip:
        filenames = zip.namelist()

        # iterate file inside the zip
        for filename in filenames:
            with zip.open(filename) as file:
                filepath = unzip_pref + filename
                if filepath not in unzipped_object_keys:
                    s3.upload_fileobj(file, bucket, filepath)

# # #input file filepath
input_path = "s3://gbatch-training-2023"

# # input_filename = os.path.basename(input_path)

# filename = lst[1]
# # output_path = os.path.join(os.path.dirname(input_path),os.path.splitext(input_filename)[0]+".csv")
output_path = "s3://gbatch-training-2023/shubham/landing"

#Convert csv.gz to csv file
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucket)

source = 'shubham/temp/'
target = 'shubham/landing/'

foldername_list = set()
for obj in my_bucket.objects.filter(Prefix = source):
    source_filename = (obj.key).split('/')[-1]
   
    # reding the file from temp
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").\
    load("s3://gbatch-training-2023/shubham/temp/{}".format(source_filename))
    
    file = source_filename
    # ls = file.split('.')
    # new_file=ls[0]+'.'+ls[1]
    yearmonth = file.split('_')[-1].split('.')[0]
    file_name=file.replace('.gz','')
    
    
    # df.to_pandas().to_csv("s3://gbatch-training-2023/shubham/landing/yearmonth/{}".format(filename))
    #write dataframe to csv
    #df.write.format("csv").option("header","true").mode("append").save("s3://gbatch-training-2023/shubham/landing/{}".format(yearmonth))
    df1=df.withColumn("Open",col("Open").cast((DoubleType()))).withColumn("High",col("High").cast((DoubleType())))\
        .withColumn("Low",col("Low").cast((DoubleType()))).withColumn("Close",col("Close").cast((DoubleType())))
    df1.toPandas().to_csv("s3://gbatch-training-2023/shubham/landing/{}/{}".format(yearmonth,file_name))
 
   
