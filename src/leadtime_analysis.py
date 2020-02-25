import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrameWriter
from functools import reduce  
from pyspark.sql.types import DateType, IntegerType
from datetime import datetime, timedelta
import psycopg2
from db_functions import *
from util import *
from s3_functions import *
import time

"""
This script is used to parse through all the cities and for each one extract the booking lead times based on historical data.
The lead times are extracted for all the listings in the city and for each listing, for all the future dates upto the year 2021.
The results are stored in database to be used in the app.
"""

def main():
    
    POSTGRES_URL = 'jdbc:postgresql://10.0.0.12:5432/postgres'
    
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

    #create a list of cities available in S3
    city_list = get_city_list('hodabnb')
    
    for city in city_list:
        
        start = time.time()
        
        #create a list of calendar files available for each city
        file_list = get_object_list (city, 'calendar.csv', 'hodabnb')
        
        #for each city fetch all the calendar files from s3
        dfs = []
        for file_name in file_list:
            scrape_date = file_name.split('_')[1]
            path = 's3n://hodabnb/' + file_name
            dfs.append(spark.read.format('com.databricks.spark.csv')\
                                    .options(header='true', inferSchema='true')\
                                    .load(path)\
                                    .select('Listing_id', 'date' , 'available')\
                                    .withColumn("scrape_date",lit(scrape_date).cast(DateType())))
                                                

        #merge all and process as one
        df_all = reduce(DataFrame.unionAll, dfs)
        df_all = df_all.withColumn("date", df_all["date"].cast(DateType()))
        df_all = df_all.withColumn("listing_id", df_all["listing_id"].cast(IntegerType()))
        df_all = df_all.withColumn('lead_time',when(df_all['available'] == 't', datediff(df_all['date'],df_all['scrape_date'])).otherwise(999))
        df_all = df_all.drop('scrape_date','available')
        df_all = df_all.dropDuplicates()
        df_all = df_all.groupBy('date','listing_id').agg({'lead_time': 'min'})
        df_city = df_all.withColumn("city",lit(city))
        
        #write to DB
        df_city.write.format("jdbc")\
                .option("url", POSTGRES_URL) \
                .option("dbtable", "leadtime_history") \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver")\
                .mode("append")\
                .save()
        
        end = time.time()

        print("finished job for %s in %s sec" %(city,(end-start)))
            
    

if __name__ == "__main__":
    main()