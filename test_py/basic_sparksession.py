import re
import datetime
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("VK_ANALYSIS") \
    .getOrCreate() \

df1 = spark.createDataFrame(
    [["Fu", 22, "Male"], ["yin", 23, "Female"], ["Fu", 30, "Male"]], ["name", "age", "gender"])
print(df1.collect())
