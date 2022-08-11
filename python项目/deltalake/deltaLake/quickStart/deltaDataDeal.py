from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime
from pyspark.shell import spark
from delta import *

inputPath = '/Users/jiang.li/Desktop/event.json'
# spark.read 读取 json数据，并将表头time装换为date格式
events = spark.read \
    .option("inferSchema", "true") \
    .json(inputPath) \
    .withColumn("date", expr("time")) \
    .drop("time") \
    .withColumn("date", from_unixtime('date', 'yyyy-mm-dd'))
# withColumn("新的列","旧的列")  如果新旧两列的名称相同，那么新的列的表达式或者值会替换旧的列

events.show()

deltaPath = '/Users/jiang.li/Desktop/event_delta.json'
#  将数据使用Delta格式写入 缺 delta jar包
events.write.format("delta").mode("overwrite").partitionBy("date").save(deltaPath)

# 再次读取数据查看是否成功保存
events_delta = spark.read.format("delta").load(deltaPath)
events_delta.printSchema()

database = "lakehouse_test";
# 重置数据库
# 注意{}是在pyspark里spark.sql()中使用的变量，参数在.format中指定
#  (参考：https://stackoverflow.com/questions/44582450/how-to-pass-variables-in-spark-sql-using-python)
spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database))

spark.sql("USE {}".format(database))
deltaPath = '/Users/jiang.li/Desktop/event_delta.json'
spark.sql("CREATE TABLE events USING DELTA LOCATION \"{}\"".format(deltaPath))