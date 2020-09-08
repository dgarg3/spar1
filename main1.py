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

sc.range(18).write.format("parquet").mode("overwrite").save("abcd")

df1 = sc.createDataFrame(range(10), s.IntegerType())

df1.write.format("parquet").mode("overwrite").save("abcd")



rdd = sc.range(18)
l = rdd.map(lambda i:True if i <=5 else sys.exit(1))
df3 = sc.createDataFrame(l)
df3.write.format("parquet").mode("overwrite").save("abcd")


df2 = sc.read.parquet('abcd')

df2.show()



