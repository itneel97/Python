from sparkDrivers import createSparkDriver
spark = createSparkDriver()

#print(spark.sparkContext.uiWebUrl)

# in spark we have 2 type of operation transformation and actions
# after transformation we get some result--> result has to be (printed or stored)

#parallelize will creates a rdd using local collection
# l1 =[23,34,45,67]
# r1 = spark.sparkContext.parallelize(l1) #creating rdd from a collection(list/ tuples/ dictionary)
# print(r1.collect())#
# t1 = ('gj','mh','rj','ph','dl')
# r2 = spark.sparkContext.parallelize(t1)
# print(r2.collect())#
# r3 = spark.sparkContext.range(10)
# print(r3.collect())

#wordCount
# textFile, map,reduceByKey, flatMap are considered as transformation
# collection, saveASTextFile are actions
rd1 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\/10_wordcount.txt")
print(rd1.collect())
rd2 = rd1.map(lambda x:x.encode('utf-8')) #convert unicode formate into string
print(rd2.collect())
# flatMap combines all words in single list so we can count word easily
# ..map(lambda x:(x,1)) ==>(x,1) groupByKey() ==> first will always be key and second will be value
# groupingbykey + aggregagating --> reducebyKey(aggrgation)
rd3 = rd2.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
#rd3.saveAsTextFile("D:\PythonAndSparkforBigDataMaster\wordcountOP")
print(rd3.collect())
# each action will have job for each job there will be DAG, submission time, details

rd4 = rd2.map(lambda x:x)
print(rd4.collect())
# lazy evaluation
# Transformation are lazily evaluated -- Transformation needs to be invoked by some actions --> transformation will be passive as long as there is no action involved

# spark default caches the data is used mostly in operations
# persistence - we can make spark to preserve large amount of data in Ram or Disk, So we can use it later
