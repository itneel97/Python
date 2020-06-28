# Checkpointing concept
# spark will be launch when the spark object is created and will stop at spark.stop()
# persistence dependent on lineage and data will be lost when spark.stop() including persisted data
# lineage information will  be in memory(RAM) and if spark application lose memory then everything will be lost
# Does persistence depend on lineage? YES
# checkpointing is not depend on lineage and it can be done only in disk
# persistence can be done in disk as well as memory
# Pipeline : sequence of execution of all the narrow transformations called pipeline
# whenever a wide transformation will encounter a new stage will created
# The stages will be created by a scheduler called as DAG scheduler - schedule the flow of execution
from sparkDrivers import createSparkDriver
spark = createSparkDriver()

spark.sparkContext.setCheckpointDir("C:\python\pyfiles\Test1\spark-warehouse\checkpoint")  # setting checkpoint location, preferred is hdfs
# and which rdd you want to checkpoint
r1 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\lordofthering.txt")
r1.checkpoint()  # check pointing rdd

# partitions - chunk of data
# executor - logical resource/unit(memory and CPU) created for processing a partition
# task - computation/processing of a partition by an executor
