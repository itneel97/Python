from sparkDrivers import createSparkDriver
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql.types import  *
spark = createSparkDriver()
from time import time


movies_rd = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\movies\/movies.dat")
rd2 = movies_rd.map(lambda x:x.encode("utf-8")).map(lambda x:x.split("::")).map(lambda x:((int)(x[0]),x[1],x[2]))
movies_df = rd2.toDF(["Movieid","Title","Generes"])
#df_movies.show(5)

#or we can try this as well
# schMovie = StructType[StructField("id",IntegerType(),False)]

ratings_rd = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\movies\/ratings.dat")
# if it gives error: u"cannot resolve '`movies_df.Movieid`' given input columns: [Generes, MoviesID, Timestemp, Rating, UserID, Title, Movieid];;\n'Join Inner, ('movies_df.Movieid = 'ratings_df.MoviesID)\n:- LogicalRDD [Movieid#0L, Title#1, Generes#2], false\n+- LogicalRDD [UserID#6L, MoviesID#7L, Rating#8L, Timestemp#9], false\n"
#starting with u then add rdd.map(lambda x:x.encode("utf-8"))....
rating_rd2 = ratings_rd.map(lambda x:x.encode("utf-8")).map(lambda x:x.split("::")).map(lambda x:((int)(x[0]),(int)(x[1]),(int)(x[2]),x[3]))
ratings_df = rating_rd2.toDF(["UserID","MoviesID","Rating","Timestemp"])
#cache() function will store result in memory hence when we need the result next time it will be loaded from memory
#easily and quickly
movies_df.cache()
ratings_df.cache()

#-----------------JOIN--------------------------------
#          Joined_df =  first_df.join(sec_df,col("first_df.firstID") == col("sec_df.secID"),TypeOfJoin)
#and syntax below will also work
#   Joined_df =  first_df.join(sec_df,col("firstID") == col("secID"),TypeOfJoin)
moviesJoinRatings_df = movies_df.join(ratings_df, F.col("Movieid") == F.col("MoviesID"),"inner")
moviesJoinRatings_df.cache()
moviesJoinRatings_df.show(5)

#This will also  work ==> moviesJoinRatings_df2 = moviesJoinRatings_df.select(["Title","UserID","Rating"]).groupBy("Title").agg(F.count("Rating").alias("Based On Rating")).orderBy(F.desc("Based On Rating")).limit(20)
#moviesJoinRatings_df2 = moviesJoinRatings_df.select(["Title","UserID","Rating"]).groupBy("Title").agg(F.count("UserID").alias("Views")).orderBy(F.desc("Views")).limit(20)
#moviesJoinRatings_df2.show(20)

moviesJoinRatings_df.createOrReplaceTempView("moviesRating")
ans1 = spark.sql('select Title,count(UserID) as views from moviesRating group by Title order by views desc limit(20) ')
ans1.coalesce(1).write.format("csv").option("header",True).save('C:\python\pyfiles\Test1\spark-warehouse\movie_usecase\question1')


