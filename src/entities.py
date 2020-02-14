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



