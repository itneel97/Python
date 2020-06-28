# we two types of transformation
# 1. Narrow and, 2. Wide
# rdd/df - internally it is a collection of partitions
from sparkDrivers import createSparkDriver
spark = createSparkDriver()
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

from pyspark import StorageLevel

# print(spark.sparkContext.uiWebUrl.encode('utf-8'))
def takeName(x):
    result = []
    for i in range(0, len(x)):
        if(x[i] == "Frodo"):
            result.append(x[i])
    return result

# print(takeName([["hi","my","name","is","Frodo","and","Frodo"],["hi","my","name","is","Frodo","and","Frodo"],["hi","my","name","is","Frodo","and","Frodo"]]))
#
spark.udf.register('demo', takeName,StringType())
demo = udf(takeName,StringType())

rd1 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\lordofthering.txt")

rd2 = rd1
rd3 = rd2.map(lambda x: x.encode("utf-8")).filter(lambda x: "Bilbo" in x)
rd4 = rd3.map(lambda x: x.split(" "))
rd5 = rd4.flatMap(lambda x: x)
print(rd5.collect())
print(rd5.getNumPartitions())
rd5 = rd4.repartition(5)
print(rd5.getNumPartitions())

# wide transformation needs shuffling





