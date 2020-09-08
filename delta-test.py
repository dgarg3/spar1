from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from delta.tables import *
#Added config so we can post spark packages
spark = SparkSession.builder\
            .master('local')\
            .appName('delta-test')\
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .getOrCreate()

dt = spark.range(1, 3)
print(dt)
#dt.write.format("delta").mode("overwrite").save("file:///mac/python/spar1/abcd")
dt.write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema","True")\
  .save("/mac/python/spar1/tp")
#writing to delta table
#dt.write.format("delta").mode("overwrite").saveAsTable("tp1")

dt1 = spark.range(5, 8)
#
#dt1 = dt1.withColumn("id",func.col("id").cast("String"))
dt1.write.format("delta").mode("append").save("tp")
## Trying to read the same parquet
#df2 = spark.read.parquet('tp')

#df2.show()
##Load data frame in to delta table
delta_df2 = DeltaTable.forPath(spark,"/mac/python/spar1/tp")
print(delta_df2.toDF())








