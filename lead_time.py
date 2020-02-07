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

from db_functions import *
from data_processing import *
from s3_functions import *


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

for city in city_list:
    
    if city == 'san-francisco':
        pass
    else:
        continue

    #create list of files avialbale for the city and assign it to NearestNeighborDict
    D = NearestNeighborDict()
    file_list = get_object_list (city, 'calendar.csv', 'hodabnb')
    for file_name in file_list:
        scrape_date = file_name.split('_')[1]
        D [scrape_date] = file_name

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
                    lead_time = -1
                    break

                availablity = check_availablity(D[i], date_1y_ago, list_id)
                if availablity == True:   
                    lead_time = (date_1y_ago - datetime.strptime(i,'%Y-%m-%d')).days
                    print(lead_time)
                    break
                if availablity == None:
                    lead_time = -2
                    break
 
            # Insert data to table

            #find the primary key of listing table for each listing
            cursor.execute("SELECT id FROM listing WHERE listing_id = %s" % list_id)
            list_table_id = cursor.fetchall()[0][0]
            print('INSERT INTO lead_time ( list_table_id, travel_date, lead_time_1y ) VALUES ( %s, DATE \'%s\', %s )' %  (list_table_id, datestr, lead_time) ) 
            cursor.execute('INSERT INTO lead_time ( list_table_id, travel_date, lead_time_1y ) VALUES ( %s, DATE \'%s\', %s )' %  (list_table_id, datestr, lead_time) )
            conn.commit() # <--- makes sure the change is shown in the database
    
    cursor.close()
    conn.close()
            

