from typing import Optional
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sch_reg import sch_reg
from os.path import abspath

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_MOVIES_TITLES_TOPIC = "numtest"
OUTPUT_MOVIES_TITLES_TOPIC = "sch_kafka_json"
STARTING_OFFSETS = "latest"
CHECKPOINT = "checkpoint"


working_directory = 'pyspark/jars/*'
warehouse_location = abspath('pyspark/warehouse/')

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config('spark.driver.extraClassPath', working_directory) \
    .config('spark.sal.warehouse.dir') \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

jsonOptions = {"timestampFormat": "yyy-MM-dd'T'HH:mm:ss.sss'7'"}

sch_reg = sch_reg

stream_sch = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", INPUT_MOVIES_TITLES_TOPIC) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .option("checkpoint", CHECKPOINT) \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), sch_reg, jsonOptions).alias("schr"))

stream_sch.printSchema()

get_col_for_stream_sch = stream_sch.select(
    col("schr.id").alias("id"),
    col("schr.farm_name").alias("farm_name")
)

# write_into_c = get_col_for_stream_sch \
#     .select('id', 'farm_name') \
#     .writeStream \
#     .format("console") \
#     .start()
#
# write_into_c.awaitTermination()
#
# print(write_into_c)

write_into_c = get_col_for_stream_sch \
    .select('id', 'farm_name') \
    .select(to_json(struct(col('id'), col('farm_name'))).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("topic", OUTPUT_MOVIES_TITLES_TOPIC) \
    .option("checkpointLocation", CHECKPOINT) \
    .option("failOnDataLoss", "false") \
    .outputMode("append") \
    .start()

write_into_c.awaitTermination()
