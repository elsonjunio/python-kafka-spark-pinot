from pyspark.sql.types import *

sch_prad = StructType([
    StructField('ARR_DISTRICT', StringType(), True),
    StructField('ARR_BEAT', StringType(), True),
    StructField('ARR_YEAR', StringType(), True),
    StructField('ARR_MONTH', StringType(), True),
    StructField('RACE_CODE_CD', StringType(), True),
    StructField('FBI_CODE', StringType(), True),
    StructField('STATUTE', StringType(), True),
    StructField('STAT_DESCR', StringType(), True),
    StructField('CHARGE_CLASS_CD', StringType(), True),
    StructField('CHARGE_TYPE_CD', StringType(), True)
])
