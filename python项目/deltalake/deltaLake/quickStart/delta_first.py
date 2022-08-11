import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data_path = "/Users/jiang.li/workspace/self/data/delta/first_table"

# # 创建表
# data = spark.range(0,5)
# print("生成的数据为："+data)
# data.write.format("delta").save(data_path)

# # 读取表
data_path = "/Users/jiang.li/workspace/self/data/delta/first_table"
df = spark.read.format("delta").load(data_path)
df.show()

# 覆盖更新表
# data = spark.range(5,10)
# data.show()
# data.write.format("delta").mode("overwrite").save(data_path)
# df = spark.read.format("delta").load(data_path)
# df.show()

# 条件更新表非全部覆盖
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark,data_path)

# 对每一个复数都加上100
deltaTable.update(
    condition=expr("id%2 == 0"),
    set = { "id":expr("id + 100")}
)
print("对每一个复数都加上100后的结果")
deltaTable.toDF().show()

# 删除每一个复数
deltaTable.delete(condition= expr("id%2 == 0"))
print("删除每一个复数")
deltaTable.toDF().show()

# upsert 新的数据
newData = spark.range(0,20)
print("新数据。。")
newData.show()
deltaTable.alias("oldData")\
    .merge(newData.alias("newData"),"oldData.id = newData.id") \
    .whenMatchedUpdate(set = { "id" : col("newData.id") }) \
    .whenNotMatchedInsert( values= {"id": col("newData.id")}) \
    .execute()
print("upsert 数据")
deltaTable.toDF().show()

