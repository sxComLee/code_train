from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime
from pyspark.shell import spark

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