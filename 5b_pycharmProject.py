from pyspark import SparkContext
from pyspark.sql import SQLContext
#The 5b has all the sparkContext example
# we can use SparkContext to create RDDs and
# we can use SparkSession to create DataFrame and RDDs --> better
#--------------------------RDD---------------------------------------------------------------
#SQLContext,HiveContext --> DataFrames

#when spark context read a file it trys to read file in a unicode format --default unicode method is UTF-8

#--------------------------------------------------------------------------------------------

sc = SparkContext(master="local",appName="demoApp",
                  conf='spark-jars.packages="org.apache.spark:spark_hive_2.11:2.4.0"')

#Creation of RDD
rdd1 = sc.textFile("D:\PythonAndSparkforBigDataMaster\lordofthering.txt")
print(rdd1.collect())

#creation DataFrame
sqlContextobj = SQLContext(sc)
dataFrameobj = sqlContextobj.read.format("text").load("D:\PythonAndSparkforBigDataMaster\lordofthering.txt") # <--This is how we can create DataFrame object

dataFrameobj.show(5)




