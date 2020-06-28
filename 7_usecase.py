from sparkDrivers import createSparkDriver
spark = createSparkDriver()

#delimiter can not be more than one char --> we can't use two char in dataFrame
df1 = spark.read.format('csv').option("delimiter",' ').load("D:\PythonAndSparkforBigDataMaster\/rddSample.txt")
# df1.show()


rd1 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\/rddSample.txt")

#print(rd1.collect()) # outPut is: [u'spark is a processing framework', u'spark is faster than mapreduce', u'spark and mr are processing frameworks']
#print(rd1.map(lambda x:type(x)).collect()) # unicode type
# here U is unicode

# when we want to apply logic to every element of data then map function used
rd2 = rd1.map(lambda x:(str)(x))
#print(rd2.collect())

rd3 = rd2.map(lambda x:x.split("##")) #rd3 will be list and will be splitted based on ##
rd4 = rd3.map(lambda x:(x[0],x[1],x[2],x[3],x[4]))

#-----if each item in RDD is tuple then we can create dataFrame-----------------
#Four Steps - RDD to DataFrame==
df1 = rd4.toDF() # to dataFrame
df1 = rd4.toDF(["c1","c2","c3"])
#-----------------------------------------------------------------------------------------------------------
#tsv file has tab with in as a delimiter
#df_tsv = spark.read.format('csv').option("inferSchema",True).option("delimiter","\t").load("D:\PythonAndSparkforBigDataMaster\Spark_DataFrames\sales_info.tsv")

from pyspark.sql.types import *
sch = StructType([StructField('id',IntegerType()),
                StructField('email',StringType()),
                StructField('Name',StringType()),
                StructField('Phone',StringType())])

df_tsv = spark.read.format('csv').schema(sch).option("inferSchema",True).option("delimiter","\t").load("D:\PythonAndSparkforBigDataMaster\Spark_DataFrames\sales_info.tsv")
rddTypeofRow = df_tsv.rdd
#Type of rdd generated out of dataFrame is type 'row'. So rddTypeofRow has type row rdd (Type is Type of object)

#now, how can you create df out of rdd which has type
df_rddTypeofRow = spark.createDataFrame(rddTypeofRow,sch)




