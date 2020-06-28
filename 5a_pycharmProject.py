from sparkDrivers import createSparkDriver
# import logging
#
if __name__ == '__main__':
#
#     #logging configuration
#     # logging levels are INFO,DEBUG,ERROR,WARN,FATAL
#     logging.basicConfig(filename="C:\python\pyfiles\logs\spark1.log", level=logging.INFO)

#     #creation of spark driver
    spark = createSparkDriver()
#     logging.info("Spark Driver created successfully created")
#
#     #reading the input parameters
#     file_format = raw_input("Enter file format  ")
#     path = raw_input("Enter Path of file    ") #D:\PythonAndSparkforBigDataMaster\demo.json
#     logging.info("Input parameters successfully imported")
#
#
#     #reading the data
#     logging.info("Reading Data started now")
#     df = spark.read.format(file_format).option("multiLine",True).load(path)
#     logging.info("Reading Completed")
#
#     #processing data
#     df.show()
#
#
#     #stop the application/job
#     logging.info("Application is Stopped")
#     spark.stop()
#     print("success")

#--------------------------RDD---------------------------------------------------------------
# we prefer to use dataFrames when we are dealing with structured or semi-structured data.
# we use RDD if the file is unstructured -- events of logs---random data ---when cleanning of data is required
# when we want to do filtering and cleaning of data dataFrame is not much efficient but RDD is
# we can do same thing with dataFrame but it will not be as efficent as RDDs.


#--------------------------------------------------------------------------------------------

rdd1 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\lordofthering.txt")
print(rdd1)

df_json = spark.read.format("json").option("multiLine",True).load("C:\\python\\pyfiles\\restapi\\thirdfile.json")
df_json.printSchema()



