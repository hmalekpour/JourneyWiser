import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrameWriter
from functools import reduce  
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import psycopg2
from db_functions import *
from util import *
from s3_functions import *

"""
This script is used to parse through all the cities and for each one extract the booking lead times based on historical data.
The lead times are extracted for all the listings in the city and for each listing, for all the future dates upto the year 2021.
The results are stored in database to be used in the app.
"""

def main():
    #constants used to establish connection to postgres database
    DB_NAME = "postgres" 
    USER_NAME = "postgres"
    HOST = "ec2-18-191-205-97.us-east-2.compute.amazonaws.com"
    PORT = 5432
    POSTGRES_URL = 'jdbc:postgresql://10.0.0.14:5432/postgres'

    #DB connection
    conn = psycopg2.connect(database = DB_NAME, user = USER_NAME, host = HOST, port = PORT)
    cursor = conn.cursor()  

    # Configure spark SQL
    conf = (SparkConf()\
            .setAppName("Process")\
            .set("spark.executor.instances", "4")\
            .set("spark.driver.memory", "50g")\
            .set("spark.executor.memory", "6g"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    spark = SparkSession.builder.appName('lead time predictor').getOrCreate()

    city_list = get_city_list('hodabnb')
    today = datetime.strptime(datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d')

    for city in city_list:
        if city == 'venice':
            pass
        else:
            continue

        df_dates = []
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
            if date.year >= 2021:
                break
            datestr = date.strftime('%Y-%m-%d')
            date_1y_ago = date - timedelta(days=364)
            datestr_1y_ago = date_1y_ago.strftime('%Y-%m-%d')
            date_stop_search = date_1y_ago - timedelta(days=120)   
            it = D(datestr_1y_ago)
            
            for i in it:
                if datetime.strptime(i, '%Y-%m-%d') < date_stop_search: 
                    break
                df = df_dict[D[i]]
                df = df.select(df['listing_id'],df['date'],df['available'])
                df = df.filter(df['date'] == date_1y_ago)
                lead_time = (date_1y_ago - datetime.strptime(i,'%Y-%m-%d')).days
                df = df.withColumn('lead_time',when(df['available'] == 't',lead_time).otherwise(999))
                df = df.select(df['listing_id'],df['lead_time'])
                df = df.withColumn("date",lit(datestr).cast(DateType()))
                dfs.append(df)
        
            #merge all processed dfs and find the minimum
            df_date = reduce(DataFrame.unionAll, dfs)
            df_date = df_date.dropDuplicates()
            #find the minimum lead_time
            df_date = df_date.groupBy('date','listing_id').agg({'lead_time': 'min'})
            #df_dates.append(df_date)
            df_date = df_date.withColumn("city",lit(city))
            #write to DB
            df_date.write.format("jdbc")\
                    .option("url", POSTGRES_URL) \
                    .option("dbtable", "lead_time_prediction") \
                    .option("user", "postgres") \
                    .option("password", "postgres") \
                    .option("driver", "org.postgresql.Driver")\
                    .mode("append")\
                    .save()
            print ('\n\n\nData was successfully written to postgresql for %s and %s \n\n\n' %(city, datestr))
            
    #close db connection          
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()



            

