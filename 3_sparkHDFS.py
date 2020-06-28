#in spark we have core we have 1) core 2) sql 3) streaming 4) ml 5)Graphx
# by default 1GB ram and 1 Core will be allocated
from pyspark.sql import SparkSession
# Spark driver creation
sparkdriver = SparkSession.builder.master('local').appName("AppNameTest").getOrCreate()

#this is how we can connnect to any remote computer( or server) and read csv file from it
# using inferSchema we can get datatype
# using header we can get header of column

# or we can define Schema of our own
#from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
# sch = StructType(
#     [StructField("id", IntegerType(), False)] #id should not be null so we are defineing it as false
#     [StructField("name", StringType(), True)] #  name can be null so it may remain null
# )
# df_hdfs  = sparkdriver.read.option("header","true").\
                        #        schema(sch).\
                        #     option("inferSchema", "True"). \
                        #     format("csv").load("hdfs://172.16.38.131:8020/foldername/filename.csv")


# print(df_hdfs.printSchema)
# df_hdfs.show(5)


#                                           option--> delimiter help to seperate data (act like split function)
#df_hdfs  = sparkdriver.read.format("csv").option('delimiter','\t').load("hdfs://172.16.38.131:8020/foldername/filename.csv")

df_hdfs  = sparkdriver.read.option("header","true").\
                               option("inferSchema", "True").\
                                format("csv").\
                                load("D:\PythonAndSparkforBigDataMaster\New folder\Course_Notes\Spark_DataFrame_Project_Exercise\walmart_stock.csv")

# df_hdfs.show(5)

# in hdfs data will be storeed on multiple node ( or we can say blocks)
#spark will try to read(by defult) as it is means if a single file is divided into 10 blocks on HDFS then spark will read data
# in 10 partitions(chunks)

# print(df_hdfs.rdd.getNumPartitions()) # it will print one partition because by default spark parallelism is set to 1, so it will read data as a single
# print(sparkdriver.sparkContext.defaultParallelism) # it  will return default parallelism

#df_hdfs.where("phoneNo = 1234567890").show()

from pyspark.sql.functions import *

#  ithColumn creates new column withColumn("newColumnName", "existringColumnName") ---> various operations can be performed in existringColumnName field as shown below...
newdf = df_hdfs.withColumn('newOpen',((df_hdfs['Open'].cast('int')/10)).cast('int'))
newdf.withColumn('finalOpen', (newdf["newOpen"]/10).cast('int')).show(5)

# distinct returns unique value from the column in this case unique company names and won't be repeated --> apple, google, dell
#df_hdfs.select("Company").distinct().show()

# when and otherwise is used as if and else
newdf.withColumn("ifAndElseCondition",when(newdf["newOpen"] == 6,"This 6").when(newdf["newOpen"] == 5,"This 5").otherwise("OK")).show()

from pyspark.sql import functions as F
# df_hdfs.select(F.col("Open")).show()

# This is how we can create a new column and can assign same value in each cell of the column
# newdf = df_hdfs.withColumn("New Column",F.lit(2002))
# newdf.show()

#This is how we can change name of existing column
#df_hdfs.withColumnRenamed("Open", "newNameofOpenColumn")

#This is how we can drop multiple column at a same time
# df_hdfs.drop("Open","High","Low").show(5)

#we can create tables out of dataFrames and we can apply pure sql on those tables --> tables can be temporary or permanent

#data stored in df_hdfs will form a table and we called it temptable1(or you can say it is instance name)
df_hdfs.registerTempTable("temptable1") # or we can use df_hdfs.creatOrReplaceTempView("temptable1")
sparkdriver.sql("select * from temptable1").show()
sparkdriver.sql("show databases").show()
sparkdriver.sql("show tables in default").show() # 'default' is a database that is created by default by spark
# So temptable1 is temporary table and will be deleted once session is closed, But we can create permanent tables as well
sparkdriver.sql("create database db1").show()
sparkdriver.sql("show databases").show()
print("Tables in db1 as below")
sparkdriver.sql("show tables in db1").show()
permanentTableDataFrame = df_hdfs.select(["Open","Close"])
permanentTableDataFrame.show()
permanentTableDataFrame.write.saveAsTable("db1.per_table2")
#permanent Table was successfully created.
sparkdriver.sql("show tables in db1").show()


#LFS --> Storage system being used by default by spark (stores data locally spark-warehouse)
#embedded derby -->metastore
