from deltaLake.util.config.ConfigureSpark import ConfigureSpark as sparkUtil

sparkSession = sparkUtil.getDeltaSparkSession("deltaTableSource")

data_path = "/Users/jiang.li/workspace/self/data/delta/"
table_name = ""

sparkSession.readStream.format("delta").load(data_path+table_name)

