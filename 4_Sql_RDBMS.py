from pyspark.sql import SparkSession

#encounters --> with date and  time,  admissions,  patients
sparkdriver = SparkSession.builder.master("local").appName('demo').\
        getOrCreate()

df_mysql  = sparkdriver.read.format("jdbc").\
            option('url','jdbc:mysql://localhost:3306').\
            option('driver','com.mysql.jdbc.Driver').\
            option('user','root').\
            option('password','8989').\
            option('dbtable','chdb.patients').\
            load()
#Both of these methods works fine...I have checked twice
# read.format("jdbc")\
# .options(url="jdbc:mysql://localhost:3306/chdb?user=root&password=8989", dbtable="items")\
# .option('driver','com.mysql.jdbc.Driver')\
# .load()
# print("success")
# df_mysql.show(5)

#multiLine can help to read file which has multiple lines
# df_mysql =  sparkdriver.read.formate('jdbc').\
#             option("multiLine", True).\
#             option(url='jdbc:mysql://localhost:3306',driver='com.mysql.jdbc.Driver', user='root' , password=8989 ,dbtable='nameofdb.table').\
#             load()

#RESTAPI --> data is received and sent from server to server or internet by calling their API
# --> reading or accessing data through a url is called RESTAPI
#using url --> fetch data --> store it into file --> load in dataFrame

import requests

#This is how you can get file directly from some URL
# jsonapidata = requests.request('GET','https://api.github.com/users/hadley/orgs')
#print(len(jsonapidata.json()))
# json_data = jsonapidata.json() #data is loaded into json_data object
#to open file we have write, append -- in write when we write something existing file will be deleted and new data and file will be added
#                                           a means append --> will append data
# file = open("C:\\python\\pyfiles\\restapi\\thirdfile.json","a" )
# for record in json_data:
#     file.write((str)(record))
# df_json = sparkdriver.read.format("json").option("multiLine",True).load("C:\\python\\pyfiles\\restapi\\thirdfile.json")
# df_json.printSchema()
#

from pyspark.sql.functions import *
#show functions --> display all the functions and we have toatl 296 fuctions
# sparkdriver.sql("show functions").show(296)
# see documentatin of functions
# sparkdriver.sql("describe function aggregate").show()

#(date_add(current_date()),1) --> to add one more day in current date
#current_timestamp() --> to date and time and seconds everything
# df1 = df_mysql.withColumn('day',current_date()) #must be same --day
#df1.write.partitionBy("day").mode("append").saveAsTable("table2")# partationBy must be same -- day
#this is how we can write file with any formate to anywhere
# df1.write.format('json').partitionBy('day').mode('append').save("C:\python\pyfiles\Test1\spark-warehouse\directfile")

# df1.show(5)
# df_mysql.show(5)

def extract(x):
    x1 = x.split('T')[0].split('-')
    year = (int)(x1[0])
    month = (int)(x1[1])
    day = (int)(x1[2])

    return[year,month,day]

print(extract('2014-03-15T212534kjhkh'))





print("success")