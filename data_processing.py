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

#create a SparkSession instance
spark = SparkSession \
            .builder \
            .appName("Spark SQL") \
            .config("") \
            .getOrCreate()

sc = pyspark.SparkContext.getOrCreate()
sql = SQLContext(sc)

#-----------------------------nearest neighbor dict--------------------------
class NearestNeighborDict(dict):
        def __init__(self):
            dict.__init__(self)
            self._keylist = []
            self.iter_index = 0

        def __getitem__(self, x):
            if x in self:
                return dict.__getitem__(self, x)
            index = bisect_left(self._keylist, x)

            if index == 0:
                raise KeyError('No prev date')
            index -= 1
            return dict.__getitem__(self, self._keylist[index])

        def __setitem__(self, key, value):
            if key not in self:
                index = bisect(self._keylist, key)
                self._keylist.insert(index, key)
            dict.__setitem__(self, key, value)

        def __iter__(self):
            while self.iter_index > 0:
                self.iter_index -= 1
               # yield dict.__getitem__(self, self._keylist[self.iter_index])
                yield self._keylist[self.iter_index]

        def __call__(self, key):
            index = bisect_left(self._keylist, key)
            if index < len(self._keylist) - 1 and self._keylist[index + 1] == key:
                self.iter_index = index + 1
            else:
                self.iter_index = index
            return self

#--------------------------- check availablity -----------------------------------
def check_availablity(file_name,date,list_id):
        #create a SparkSession instance
        spark = SparkSession \
                .builder \
                .appName("Spark SQL") \
                .config("") \
                .getOrCreate()

        sc = pyspark.SparkContext.getOrCreate()
        sql = SQLContext(sc)
        
        path = 's3n://hodabnb/' + file_name
        df = spark.read.format('com.databricks.spark.csv').options(header='true', inferSchema='true').load(path)
        df = df.select(df['listing_id'],df['date'],df['available'])
        df = df.filter(df['listing_id'] == list_id)
        df = df.filter(df['date'] == date)
        df = df.select(df['available'])
        if len(df.head(1)) > 0:
            availablity = df.select(df['available']).collect()[0]['available']
            if availablity == 't':
                return(True)
            elif availablity == 'f':
                return(False)
        else:
            return(None)

#------------------------------for a given listing id  generate furure date up to one year from  now and add it to lead_time table----------------
#def generate_future_date(listing_id)
#    today = datetime.now()
#    for i in range(365)+1:
#        future = today + timedelta(days=i)
        
