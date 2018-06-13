from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf, SparkContext


def __init__copy(self, useDisk, useMemory, useOffHeap, deserialized, replication=1):
    self.useDisk = useDisk
    self.useMemory = useMemory
    self.useOffHeap = useOffHeap
    self.deserialized = deserialized
    self.replication = replication


sc = SparkContext()
kvRDD = sc.parallelize([(3, 5), (1, 2), (2, 4)])
# Cache KVRDD
kvRDD.persist()
# Di Cache KVRDD
kvRDD.unpersist()
