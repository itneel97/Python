# persistence - for reuse rdd/df
# rdd/df -- memory/disk serialized, with/without replication.
# memory/disk -- memory

# storage(writing) - serialization will be used.
# read data - deserialization

# serialization saves space
# deserialization saves time

# object can be used for processing only in deserialized mode

# in python stored objects will always be serialized with the  pickle library
# serialization and deserialization are not supported in python

# serialization -- data sized gets reduced --JVM objects converting into binary object
# deserialization - data size will be as it is -- binary to JVM object0

# data will be by default in deserialized mode
# serialization wil take some time,

# under which situation objects get serialized?
# 1. storing/caching
# 2. when objects are transferred over the network(mandatory)


from sparkDrivers import createSparkDriver
spark = createSparkDriver()
from pyspark import StorageLevel


rd1 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\/10_wordcount.txt")
rd2 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\/10_wordcount.txt")
rd1.persist(StorageLevel.MEMORY_ONLY)
rd2.persist(StorageLevel.MEMORY_ONLY_SER)
print(rd1.count())
print(rd2.count())
# to see partitions
print(rd1.getNumPartitions())





