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
import time
from db_functions import *
from data_processing import *
from s3_functions import *
import os
from pyspark.sql.functions import lit
from pyspark.sql import DataFrameWriter

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '10.0.0.14')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_URL = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

#-------------fill city table-------------
#city = get_city_list('hodabnb')

#for c in city:
#        db_insert('INSERT INTO city(name) VALUES(\'%s\')' % c)
#--------------fill listing table------------

#----------------------------------------------------------------------------------------------

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
    
    if city == 'antwerp':
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

    #find the primary key of city table for each city file
    cursor.execute("SELECT id FROM city WHERE name = \'%s\'" % city)
    city_id = cursor.fetchall()[0][0]

    cursor.execute("SELECT listing_id FROM listing WHERE city_id = %s" %city_id)
   
    listings = cursor.fetchall()

    for listing in listings:
        list_id = listing[0]
        #find the primary key of listing table for each listing
        cursor.execute("SELECT id FROM listing WHERE listing_id = %s" % list_id)
        list_table_id = cursor.fetchall()[0][0] 

        for d in range(1,365):
            date = today + timedelta(days=d)
            datestr = date.strftime('%Y-%m-%d')
            date_1y_ago = date - timedelta(days=364)
            datestr_1y_ago = date_1y_ago.strftime('%Y-%m-%d')
            date_stop_search = date_1y_ago - timedelta(days=120)
            
            it = D(datestr_1y_ago)
            for i in it:
                if datetime.strptime(i, '%Y-%m-%d') < date_stop_search: 
                    lead_time = -1 #set for all listings
                    break
                start = time.time()
                df = df_dict[D[i]]
                df = df.select(df['listing_id'],df['date'],df['available'])
                df = df.filter(df['date'] == date_1y_ago)
                df = df.select(df['listing_id'],df['available'])
                df = df.filter(df['available'] == 't')
                lead_time = (date_1y_ago - datetime.strptime(i,'%Y-%m-%d')).days
                print('lead time %s' %lead_time)
                df_available = df.withColumn("lead_time",lit(lead_time))
                #df_available.show(20, False)
                before = time.time()
                #data_to_db = df_available.collect()
                #for item in data_to_db:
                #    if len(item) < 3:
                #        print(item)
                df = df_available.na.drop()
                df.write.jdbc(
                    url = POSTGRES_URL, 
                    table = 'lead_time2', 
                    mode = 'ignore', 
                    properties = {'user':'postgres', 'password': 'postgres','driver': 'org.postgresql.Driver'})
                #data_to_db = df.collect()
                end = time.time()
                
                print('\n\n\nprocess time: ')
                print(end-start)
                
            
            
            # Insert data to table
            #cursor.execute("SELECT id FROM listing WHERE listing_id = %s" % list_id)
            #list_table_id = cursor.fetchall()[0][0]
            #cursor.execute('INSERT INTO lead_time ( list_table_id, travel_date, lead_time_1y ) VALUES ( %s, DATE \'%s\', %s )' %  (list_table_id, datestr, lead_time) )
            #conn.commit() # <--- makes sure the change is shown in the database
    cursor.close()
    conn.close()
            

