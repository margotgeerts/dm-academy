import io
import boto3
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import configparser
import os
import json
from pyspark.sql.functions import col

#add JAR to spark session 
spark = SparkSession.builder.appName('my_app').config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2').getOrCreate()


#get aws credentials, not necessary, in aws configure
#access_key = os.environ['AWS_ACCESS_KEY']
#secret_key = os.environ['AWS_SECRET_KEY']
#spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
#spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

#Make s3 a recongnizable file system
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

s3_folder = 's3a://dataminded-academy-capstone-resources/raw/open_aq/'

s3_df=spark.read.json(s3_folder+"data_part_1.json")

def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [psf.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

def cast_timestamps(df, time_cols):
    for c in time_cols:
        df = df.withColumn(c, psf.to_timestamp(df[c]))
    return df

s3_df = flatten_df(s3_df)
s3_df = cast_timestamps(s3_df, ['date_local', 'date_utc'])

client = boto3.client('secretsmanager')

response = client.get_secret_value(
    SecretId='snowflake/capstone/login'
)

snowflake_secrets = json.loads(response['SecretString'])

