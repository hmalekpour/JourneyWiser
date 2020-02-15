import boto3
import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
import psycopg2
from s3_functions import *

"""
This script is used to collect the listings geographical coordinates from the most recently updated objects in S3 buckets and store them in database.
This is specially used for future development of JourneyWiser project to create a map of the calculated lead_times.
"""

def find_max_date(datestr_list):
    """
    gets a list of dates in string format and returns the most recent one.
    ...

    Parameters:
    -----------
    datestr_list : list
        list containing dates in string format.

    Returns
    --------
    maxstr : str
        string containing the most recent date found
    """
    max = datetime.strptime('2000-01-01', '%Y-%m-%d') #set the initial max value to an old datetime object (year 2000)
    for datestr in datestr_list:
        date = datetime.strptime(datestr, '%Y-%m-%d')
        if date > max:
            max = date #keep updating the max each time a most recent date is found
    maxstr = max.strftime('%Y-%m-%d')
    return(maxstr)

def main:
    
    #constants used to establish connection to postgres database
    DB_NAME = "postgres" 
    USER_NAME = "postgres"
    HOST = "ec2-18-191-205-97.us-east-2.compute.amazonaws.com"
    PORT = 5432

# Create a SparkSession instance
    spark = SparkSession \
                .builder \
                    .appName("Spark SQL") \
                        .config("") \
                            .getOrCreate()


    sc = pyspark.SparkContext.getOrCreate()
    sql = SQLContext(sc)

    #DB connection
    conn = psycopg2.connect(database = DB_NAME, user = USER_NAME, host = HOST, port = PORT)

    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('hodabnb')

    city_list = get_city_list('hodabnb')
    city_date = {}

    for city in city_list:
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
        
        df = spark.read.format( 'com.databricks.spark.csv') \
                .options (header='true', multiline = 'true', quote= '"', escape = '"', delimiter = ',' ) \
                .load(path)

        df = df.select(df['id'],df['longitude'],df['latitude'])

        for row in df.rdd.collect(): 
            print(row)
            # Insert data to db table
            cursor.execute('INSERT INTO listing ( city_id, listing_id, longitude, latitude ) VALUES ( %s, %s, %s, %s )' %  ( city_id, row['id'], row['longitude'], row['latitude']) )
            conn.commit() # <--- makes sure the change is shown in the database
    
    #close curser
    cursor.close()
    conn.close()  

if __name__ == "__main__":
    main()

