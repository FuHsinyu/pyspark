from pyspark import SparkConf, SparkContext
from operator import add
import re


def make_pair(word):
    return (word, 1)


def word_count(wordRDD):
    countedRDD = wordRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
    return countedRDD


def remove_punc(word):
    word = re.sub('[\W\_]+', '', word.lower())
    return word


import os
working_dir = os.getcwd()
input_file = os.path.join("pyspark", "test.txt")
file_name = os.path.join(working_dir, input_file)

sc = SparkContext()
textRDD = sc.textFile(file_name, 8).flatMap(lambda x: x.split()).map(lambda x: (
    remove_punc(x), 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1])
# textRDD = textRDD.map(lambda x: x.split())
print(textRDD.take(150))
