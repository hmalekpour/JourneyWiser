import boto3
import io
import pandas as pd
import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from bisect import bisect, bisect_left
import psycopg2


#-----------------------------bucket object list creator---------------------------------------
def get_object_list (city, file_ending, bucket):
    obj = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for object in bucket.objects.all():
       if object.key.startswith(city) and object.key.endswith(file_ending):
            obj.append(object.key)
       else:
            continue
    return(obj)
#---------------------------------city list creator -----------------------------------------
def get_city_list(bucket):
    city = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for object in bucket.objects.all():
        c = object.key.split('_')[0]
        if c not in city:
            city.append(c)
    return(city)
#-----------------------------------------------------------------------------------
