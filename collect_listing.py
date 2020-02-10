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
from s3_functions import *

def find_max_date(datestr_list):
    max = datetime.strptime('2000-01-01', '%Y-%m-%d')
    for datestr in datestr_list:
        date = datetime.strptime(datestr, '%Y-%m-%d')
        if date > max:
            max = date
    maxstr = max.strftime('%Y-%m-%d')
    return(maxstr)


# Create a SparkSession instance
spark = SparkSession \
            .builder \
                .appName("Spark SQL") \
                    .config("") \
                        .getOrCreate()


sc = pyspark.SparkContext.getOrCreate()
sql = SQLContext(sc)

#DB connection
conn = psycopg2.connect(database="postgres", user="postgres", host="ec2-18-191-205-97.us-east-2.compute.amazonaws.com", port=5432)

# create a psycopg2 cursor that can execute queries
cursor = conn.cursor()

s3 = boto3.resource('s3')
bucket = s3.Bucket('hodabnb')

city_list = get_city_list('hodabnb')
city_date = {}

for city in city_list:
    if city == 'antwerp':
        pass
    else:
        continue

    city_date[city]=[]
    for object in bucket.objects.all():
        if object.key.endswith('listings.csv') and object.key.startswith(city):  
            city_date[city].append(object.key.split('_')[1])
   
    most_recent_date = find_max_date (city_date[city])
    most_recent_file_name = city + '_' + most_recent_date + '_' + 'listings.csv'
    
    #find the primary key of city table for each city file
    cursor.execute("SELECT id FROM city WHERE name = \'%s\'" % city)
    city_id = cursor.fetchall()[0][0]
         
    path = 's3n://hodabnb/' + most_recent_file_name
    
    print(path)
    print('\n\n')

    df = spark.read.format( 'com.databricks.spark.csv') \
            .options (header='true', multiline = 'true', quote= '"', escape = '"', delimiter = ',' ) \
            .load(path)

    df = df.select(df['id'],df['longitude'],df['latitude'])

    for row in df.rdd.collect(): 
        print(row)
        # Insert data to table
        cursor.execute('INSERT INTO listing ( city_id, listing_id, longitude, latitude ) VALUES ( %s, %s, %s, %s )' %  ( city_id, row['id'], row['longitude'], row['latitude']) )
        conn.commit() # <--- makes sure the change is shown in the database
    #print(city + ' job done!')
#close curser
cursor.close()
conn.close()  
