import os
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import types as s

import sys
t = [0,1,2,3,4]

#conf = SparkConf().setMaster("local").setAppName("testapp")
#ct = SparkContext(conf)

sc = SparkSession.builder\
        .master('local')\
        .appName("timepass")\
        .getOrCreate()

df1 = sc.read.csv('tt1.txt')

#df = sc.createDataFrame(t,s.IntegerType())



df1.write.format('parquet').mode('append').save('abc')

df2 = sc.read.parquet('abc')

df2.show()



