#hive spark integration
#---copy 3 files into  spark ---two from spark and one from hadoop

#hive -- warehouse
#hive --> data(warehouse)(HDFS) --> except metadata all data will be stored on HDFS
#data about the table, the meta information about tables ,databases and schema whatever we have in Hive will be store on metastore
#     -->metastore(RDBMS)(mysql,durby,postgresql are databases supported)
#data on hdfs on the top of that data if you want to use sql language to perform oprations on data Hive is a tool
#Hive Language -Hql which is similar to sql
# in hive single record updation is not recommended
#Analysis purpose
#backend file system - HFS
# metadata --> RDBMS

#oracle
#single record can be modified or update
#transaction purpose
#backend file system - ext4,NTFS

#DSL --> domain specific language

# Why we need hive?
# Spark use HDFS to store data. And in HDFS data is stored as a file not as a table, Now spark can't directly represent that file as a tabular format so that,
#we need HiveData and Schema to convert file format into tabular format

#internal table
# when schema is already define (it like append)

# external table
# when you are defining schema and data.
#You are creating whole new table

# Hive has two major things hive warehouse and hive Metastore
# hive warehouse has data and hive metastore has all metadata (schema)
#and thirft is protocol using which client will establish connection to metastore server