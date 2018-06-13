import re
import datetime
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession

data_saving_dir = "/media/hsin/16FC55FBFC55D619/linux-ubuntu-shared-data/small_data/"
spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("VK_ANALYSIS") \
    .getOrCreate() \

df = spark.createDataFrame(
    [["FU", 24, 'ITMO'], ["LUO", -11, "XMUT"], ["FU", 22, "ssd"]], ["name", "age", "uni"])
df2 = spark.createDataFrame(
    [["FU", 175, 'BIGDATA']], ["name", "height", "major"])

df.join(df2, df.name == df2.name, "inner").show()
df.join(df2, "name","outer").show()
