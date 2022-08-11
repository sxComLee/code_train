from pyspark.shell import spark
from pyspark.sql.types import *

inputPath = '/databricks-datasets/structured-streaming/events/'

jsonSchema = StructType([StructField("time",TimestampType(),True),StructField("action",StringType(),True)])

eventsDF = (
    spark
    .readStream
    .schema(jsonSchema) # set the schema of the JSON Data
    .option("maxFilesPerTrigger",1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(input)
)

(eventsDF.writeStream
    .outputMode("append")
    .option("checkpointLocation","/mnt/delta/events/_checkpoints/etl-from-json")
    .table("events")
)
