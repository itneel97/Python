from sparkDrivers import createSparkDriver
spark = createSparkDriver()
print(spark.sparkContext.uiWebUrl)

# Resilience: Lineage, DAG, persistence
# Resilience: means rolling back to original state
# r1 = spark.sparkContext.textFile("some_Path")
# r2 = r1.anyTransformation()
# r3 = r2.anyTransformation()
# r4 = r3.anyTransformation()
# r5 = r4.anyTransformation()
# r5.someAction()
# r5 - r4 - r3 - r2 - r1 - textFile (tracing the dependency (flow))
# textFile - r1 - r2 - r3 - r4 - r5  (Execution of the flow)

# spark instead of executing a transformation, it will remembers the transformation to be applied(or steps).
#spark remembers the dependency of transformation in the form of a graph called as lineage
# lineage: the order of execution of rdd/df the order of transformation that are performed on a dataset that will be remember by spark
# the execution of the transformation will be represented as a graph called DAG

# where lineage is stored and how long?
# memory and as long as the application is active.

# why spark is not executing this line when it read a line? This line -->r1 = spark.sparkContext.textFile("some_Path")

# persistence is only a request not a demand
# from pyspark import StorageLevel
# rdd.persist(StorageLevel.MEMORY_ONLY) # rdd will be persisted to memory
# rdd.unpersist() # To release storage
# using persistence we are trying to achieve reuse of data which can help to increase performance
# data can be persisted on disk or memory, serialized or  deserialize, with or without replication

#rdd.persist(StorageLevel.--wehave11Options)
# MEMORY_ONLY - memory,deser,1 copy
# MEMORY_ONLY_SER - momory,serial, 1
# DISK_ONLY - disk,serial,1
# MEMORY_AND_DISK - if space in memory then memory,deser,1 copy else disk,seri,1
# MEMORY_AND_DISK_SER - if space in memory then memory,ser,1 copy else disk,seri,1
#
# MEMORY_ONLY_2, MEMORY_ONLY_SER_2, DISK_ONLY_2, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER_2
# OFF_HEAP

