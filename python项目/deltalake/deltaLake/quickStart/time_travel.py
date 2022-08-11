import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data_path = "/Users/jiang.li/workspace/self/data/delta/first_table"

# df = spark.read.format("delta").option("versionAsOf",0).load(data_path)
# print("第一个版本数据")
# df.show()

timestamp_string = '2022-08-10 20:57:00'

df = spark.read.format("delta").option("timestampAsOf",timestamp_string).load(data_path)
print("指定时间数据")
df.show()