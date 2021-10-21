from pyspark.sql.types import *

sch_reg = StructType([
    StructField('id', IntegerType(), True),
    StructField('description', StringType(), True),
    StructField('login', StringType(), True),
    StructField('farm_id', IntegerType(), True),
    StructField('farm_name', StringType(), True),
    StructField('processed', StringType(), True),
    StructField('submitted_at', StringType(), True),
    StructField('created_at', StringType(), True),
    StructField('latitude', FloatType(), True),
    StructField('longitude', FloatType(), True),
    StructField('real_latitude', FloatType(), True),
    StructField('real_longitude', FloatType(), True),
    StructField('forced_location', BooleanType(), True),
    StructField('area_id', IntegerType(), True),
    StructField('issue_id', IntegerType(), True),
    StructField('deleted', BooleanType(), True),
    StructField('synced_at', StringType(), True)
])
