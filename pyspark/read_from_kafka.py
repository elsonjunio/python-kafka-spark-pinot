from typing import Optional
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from schema import schema_prad
from os.path import abspath

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "prad"
OUTPUT_TOPIC = "prad2"
STARTING_OFFSETS = "latest"
CHECKPOINT = "checkpoint"


working_directory = 'pyspark/jars/*'
warehouse_location = abspath('pyspark/warehouse/')

spark = SparkSession \
    .builder \
    .appName("readFromKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .config('spark.driver.extraClassPath', working_directory) \
    .config('spark.sal.warehouse.dir') \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

jsonOptions = {"timestampFormat": "yyy-MM-dd'T'HH:mm:ss.sss'7'"}

stream_sch = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .option("checkpoint", CHECKPOINT) \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), schema_prad, jsonOptions).alias("schr"))

stream_sch.printSchema()

get_col_for_stream_sch = stream_sch.select(
    col("schr.ARR_DISTRICT").alias("ARR_DISTRICT"),
    col("schr.ARR_BEAT").alias("ARR_BEAT"),
    col("schr.ARR_YEAR").alias("ARR_YEAR"),
    col("schr.ARR_MONTH").alias("ARR_MONTH"),
    col("schr.RACE_CODE_CD").alias("RACE_CODE_CD"),
    col("schr.FBI_CODE").alias("FBI_CODE"),
    col("schr.STATUTE").alias("STATUTE"),
    col("schr.STAT_DESCR").alias("STAT_DESCR"),
    col("schr.CHARGE_CLASS_CD").alias("CHARGE_CLASS_CD"),
    col("schr.CHARGE_TYPE_CD").alias("CHARGE_TYPE_CD")
)


def foreach_batch_function(df, epoch_id):
    print("========= %s =========" % str(epoch_id))
    print(f"{df.count()}")
    df.show(100)


# write_into_c = get_col_for_stream_sch \
#    .select("ARR_DISTRICT", "ARR_BEAT", "ARR_YEAR", "ARR_MONTH", "RACE_CODE_CD", "FBI_CODE", "STATUTE", "STAT_DESCR", "CHARGE_CLASS_CD", "CHARGE_TYPE_CD") \
#    .writeStream \
#    .format("console") \
#    .start()
#
# write_into_c.awaitTermination()

write_into_c = get_col_for_stream_sch \
    .select("ARR_DISTRICT", "ARR_BEAT", "ARR_YEAR", "ARR_MONTH", "RACE_CODE_CD", "FBI_CODE", "STATUTE", "STAT_DESCR", "CHARGE_CLASS_CD", "CHARGE_TYPE_CD") \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .start()

write_into_c.awaitTermination()
