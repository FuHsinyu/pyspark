from pyspark import SparkConf, SparkContext
sc = SparkContext()
# Create RDD
intRDD = sc.parallelize([3, 1, 2, 5, 5])
stringRDD = sc.parallelize(["apple", "bananna", "orange", "grape", "melon"])
# Transform to Python data structure
print(intRDD.collect())
