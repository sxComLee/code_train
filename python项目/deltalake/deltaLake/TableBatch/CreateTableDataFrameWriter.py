from deltaLake.util.config.ConfigureSpark import ConfigureSpark as sparkUtil

sparkSession = sparkUtil.getDeltaSparkSession("createTable")

# 通过使用DtaFrame的schema，在元存储中创建表
tableName = 'dataFrameWriterTableTest01'
df = sparkSession.range(0,5)
print("生成的数据为："+df)
df.write.format("delta").saveAsTable("default."+tableName)

# 创建或者怪分区表，通过使用DtaFrame的schema，写入或者覆盖数据到指定路径
data_path = "/Users/jiang.li/workspace/self/data/delta/"
df.write.format("delta").mode("overwrite").save(data_path+"default."+tableName)