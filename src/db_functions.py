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


def db_insert(command):

    #DB connection
    conn = psycopg2.connect(database="postgres", user="postgres", host="ec2-18-191-205-97.us-east-2.compute.amazonaws.com", port=5432)

    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()

    # Insert data to table
    cursor.execute(command)
    conn.commit() # <--- makes sure the change is shown in the database

    #close curser
    cursor.close()
    conn.close()

def db_get(command):

        #DB connection
        conn = psycopg2.connect(database="postgres", user="postgres", host="ec2-18-191-205-97.us-east-2.compute.amazonaws.com", port=5432)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # Insert data to table
        cursor.execute(command)
        rows = db.fetchal()

        #close curser
        cursor.close()
        conn.close()

        return (rows)
