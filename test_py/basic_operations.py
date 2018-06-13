from pyspark import SparkConf, SparkContext
sc = SparkContext()
# BASIC OPERATIONS
# Create RDD
intRDD = sc.parallelize([3, 1, 2, 5, 5])
stringRDD = sc.parallelize(["apple", "banbanna", "orange", "grape", "melon"])
# Transform to Python data structure
print("transform to list", intRDD.collect())
# Map func
print(intRDD.map(lambda x: x+1).collect())
# Filter fun
print(intRDD.filter(lambda x: x < 3).collect())
# Distinct fun (remove repeated elements)
print(intRDD.distinct().collect())
# RandomSplit SPlit the RDD to RDDs with ratio 0.4 and 0.6
sRDD = intRDD.randomSplit([0.4, 0.6])
print("randomsplit1:", sRDD[0].collect())
print("randomsplit2:", sRDD[1].collect())
# Group by (sort data to multi arrays)
result = intRDD.groupBy(lambda x: x % 2).collect()
print("groupby:", [[x, sorted(y)] for x, y in result])

# MULTI RDDS TRANSFORMATION
# Create 3 RDDs
intRDD1 = intRDD
intRDD2 = sc.parallelize([5, 6])
intRDD3 = sc.parallelize([2, 7])
# Union operation
print("union:", intRDD1.union(intRDD2).union(intRDD3).collect())
# Intersection
print("inatersection:", intRDD1.intersection(intRDD2).collect())
# Substract
print("substract:", intRDD1.subtract(intRDD2).collect())
# Cartesian
print("Cartesian:", intRDD1.cartesian(intRDD2).collect())

# BASIC ACTIONS
# Read
print("first:", intRDD.first())
print("take 2", intRDD.take(2))
print("take ordered 3", intRDD.takeOrdered(3))
print("take reversed order 3", intRDD.takeOrdered(3, lambda x: -x))
# Statistics func
print("stats:", intRDD.stats())
print("Min:", intRDD.min())
print("Max:", intRDD.max())
print("sum:", intRDD.sum())

# RDD KEY TO VALUE
# Create kv RDD with tuple, first is key (3) second is value (4)
kvRDD1 = sc.parallelize([(3, 4), (3, 6), (5, 6), (1, 2)])
# Get key and value
print("keys:", kvRDD1.keys().collect())
print("vals:", kvRDD1.values().collect())
# FIlter
print("filter:", kvRDD1.filter(lambda x: x[0] < 4).collect())
print("mapvalue:", kvRDD1.mapValues(lambda x: x**2).collect())
# Sort by key
print("sort by key:", kvRDD1.sortByKey(True).collect())
# Reduce by key Union the data with same key
print("reduce by key:", kvRDD1.reduceByKey(lambda x, y: x+y).collect())

# Multi RDDS KEY VALUE TRANSORMATIONS
# Create kvrdds
kvRDD2 = sc.parallelize([(3, 8)])
# Join in inner join
print("inner join", kvRDD1.join(kvRDD2).collect())
# JOin in left outer join
print("left outer join", kvRDD1.leftOuterJoin(kvRDD2).collect())
# Join in right outer join
pass
# Join in substract, delete the same keys
print("substract join", kvRDD1.subtractByKey(kvRDD2).collect())

# KEY VALUES ACTIONS
# Count by key
print("count by key(value):", kvRDD1.countByKey())
# Find value by key
print("value of key 3:",kvRDD1.lookup(3))


