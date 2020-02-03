import boto3
import io
import pandas as pd
import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from bisect import bisect, bisect_left


#-----------------------------find file--------------------------
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
def check_availablity(file_name,date):
        path = 's3n://hodabnb/' + file_name
        df = spark.read.options(header='true', inferSchema='true').load(path,format='csv')
        df = df.select(df['listing_id'],df['date'],df['available'])
        df = df.filter(df['listing_id'] == list_id)
        df = df.filter(df['date'] == date.strftime('%Y-%m-%d %H:%M:%S'))
        df = df.select(df['available'])
        availablity = df.select(df['available']).collect()[0]['available']
        if availablity == 't':
            return(True)
        elif availablity == 'f':
            return(False)
        else:
            return(None)

#-----------------------------bucket object list creator---------------------------------------
def get_object_list(city,file_type):
    obj = []
    file_type = file_type + '.csv'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('hodabnb')
    for object in bucket.objects.all():
       if object.key.startswith(city) and object.key.endswith(file_type):
            obj.append(object.key)
       else:
            continue
    return(obj)
#-----------------------------------------------------------------------------------------------
#create a SparkSession instance
spark = SparkSession \
            .builder \
                .appName("Spark SQL") \
                    .config("") \
                        .getOrCreate()

#user inputs
trip_date_str = '2020-03-05'
list_id = '20168'
city = 'amsterdam'


sc = pyspark.SparkContext.getOrCreate()
sql = SQLContext(sc)

#find the last year date
trip_date_obj = datetime.strptime(trip_date_str, '%Y-%m-%d')
last_year_obj = trip_date_obj - timedelta(days=365)
six_month_ago_obj = last_year_obj - timedelta(days=180)
last_year_str = last_year_obj.strftime('%Y-%m-%d')


#find the file to look
D = NearestNeighborDict()
calendar_files = get_object_list (city, 'calendar')
listings_file = get_object_list (city, 'listings')

for file_name in calendar_files:
    file_date = file_name.split('_')[1]
    D [file_date] = file_name


it = D(last_year_str)
for i in it:
    if datetime.strptime(i, '%Y-%m-%d') < six_month_ago_obj: 
        break
    availablity = check_availablity(D[i],last_year_obj)
    print('\n\n'+i)
    print(availablity)

