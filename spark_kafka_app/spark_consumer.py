# Subscribe to 1 topic, with headers
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import findspark
findspark.init() 

spark = SparkSession \
    .builder \
    .appName("pdb data") \
    .master('local[*]') \
    .getOrCreate()
cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print(f"You are working with {cores} core(s)")       


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "NM") \
  .option("includeHeaders", "true") \
  .load()

# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")

df.show()