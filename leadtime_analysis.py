import boto3
import io
import pandas as pd
import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import *
from datetime import datetime, timedelta
from bisect import bisect, bisect_left
import psycopg2
import time
from db_functions import *
from data_processing import *
from s3_functions import *
import os
from pyspark.sql.functions import lit
from pyspark.sql import DataFrameWriter
from functools import reduce  
from pyspark.sql.types import DateType

POSTGRES_URL = 'jdbc:postgresql://10.0.0.14:5432/postgres'


#DB connection
conn = psycopg2.connect(database="postgres", user="postgres", host="ec2-18-191-205-97.us-east-2.compute.amazonaws.com", port=5432)
cursor = conn.cursor()  


city_list = get_city_list('hodabnb')
today = datetime.strptime(datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')

#spark session
spark = SparkSession \
        .builder \
        .appName("Spark SQL") \
        .config("") \
        .getOrCreate()
        
sc = pyspark.SparkContext.getOrCreate()
sql = SQLContext(sc)   



for city in city_list:
    
    if city == 'san-francisco':
        print('\n\n\n' + city)
        pass
    else:
        continue

    #create list of files avialbale for the city and assign it to NearestNeighborDict
    D = NearestNeighborDict()
    file_list = get_object_list (city, 'calendar.csv', 'hodabnb')
    for file_name in file_list:
        scrape_date = file_name.split('_')[1]
        if int(scrape_date.split('-')[0])> 2017:
            print('\n' + file_name)
            D [scrape_date] = file_name
    
    #fetch all the file from s3
    df_dict = {}
    for file_name in file_list:
        scrape_date = file_name.split('_')[1]
        if int(scrape_date.split('-')[0])> 2017:
            path = 's3n://hodabnb/' + file_name
            df_dict[file_name]  = spark.read.format('com.databricks.spark.csv').options(header='true', inferSchema='true').load(path)

    
    for d in range(1,365):
        dfs = []
        date = today + timedelta(days=d)
        datestr = date.strftime('%Y-%m-%d')
        date_1y_ago = date - timedelta(days=364)
        datestr_1y_ago = date_1y_ago.strftime('%Y-%m-%d')
        date_stop_search = date_1y_ago - timedelta(days=180)   
        it = D(datestr_1y_ago)
        
        for i in it:
            if datetime.strptime(i, '%Y-%m-%d') < date_stop_search: 
                break
            
            df = df_dict[D[i]]
            df = df.select(df['listing_id'],df['date'],df['available'])
            df = df.filter(df['date'] == date_1y_ago)
            df = df.filter(df['available'] == 't')
            lead_time = (date_1y_ago - datetime.strptime(i,'%Y-%m-%d')).days
            df = df.select(df['listing_id'])
            df = df.withColumn("date",lit(datestr).cast(DateType()))
            df = df.withColumn("lead_time",lit(lead_time))
            dfs.append(df)
       
        t1 = time.time()
        #merge all processed dfs and find the minimum
        df_all = reduce(DataFrame.unionAll, dfs)

        #find the minimum lead_time
        df_final = df_all.groupBy('date','listing_id').agg({'lead_time': 'min'})
        #df_final.show()

        t2 = time.time()

        #write to DB
        df_final.write.format("jdbc")\
                .option("url", POSTGRES_URL) \
                .option("dbtable", "prediction") \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver")\
                .mode("append")\
                .save()
        
        t3=time.time()

        print('\n\n\nData was successfully written to postgresql for %s and %s\n\n\n' %(city, datestr) )
        print ( t2 - t1)
        print ( t3 - t2)


            
            
           
           
cursor.close()
conn.close()
            

