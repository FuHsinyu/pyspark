import re
import datetime
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession


def read_dir_names(par_dir_path):
    dir_names = list(os.popen("ls "+par_dir_path))
    for n in range(len(dir_names)):
        if "\n" in dir_names[n]:
            dir_names[n] = dir_names[n][:-1]
    return dir_names


def count_for_each_dir(dir_name, par_dir_path):
    complete_dir_name = par_dir_path + "/" + dir_name
    # print(complete_dir_name)
    df = spark.read.parquet(complete_dir_name)
    return df.count(), dir_name, df.take(2)


spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("VK_ANALYSIS") \
    .getOrCreate() \
    #.config("master", "local[4]") \
par_dir_path = '/media/hsin/16FC55FBFC55D619/linux-ubuntu-shared-data/small_data'

dir_names = read_dir_names(par_dir_path)

for dir_name in dir_names:
    count, name, examples_2 = count_for_each_dir(dir_name, par_dir_path)
    #print("{} count is {}:".format(name, count))
    print(name, examples_2)
