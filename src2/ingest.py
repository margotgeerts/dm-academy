import pandas as pd
import requests
import warnings
import io
import boto3
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import configparser
import os
import json
from pyspark.sql.functions import col

params = {'date_from': '2000-01-01T00:00:00+00:00', 'date_to': '2022-02-11T15:01:00+00:00', 'limit': '100',
'page': '1', 'offset': '0', 'sort': 'desc', 'radius' : '1000', 'country_id': "BE", 'orderby': 'datetime'}

r = requests.get('https://docs.openaq.org/v1/measurements', params=params)

print(r.json())

s3 = boto3.resource('s3')
s3object = s3.Object('dataminded-academy-capstone-resources/Margot/ingest', 'openaq-margot.json')

s3object.put(
    Body=(bytes(json.dumps(r.json()).encode('UTF-8')))
)

# with open('json_data.json', 'w') as outfile:
#     json.dump(r.json(), outfile)

# s3 = boto3.client("s3")

# s3.upload_file(
#     r,
#     Bucket="dataminded-academy-capstone-resources",
#     Key="openaq-margot.json",
# )