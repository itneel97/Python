# ---------------------------------------------------------------------------------------------------------------------
# all of this are actions
# reduce(f1) -- f1 will be applied on partitions and results of partitions
# fold(zerovalue,f1) -- f1 will be applied on partitions and result of partitions with zerovalue
# aggregation(zerovalue, f1,f2) --  f1 will be applied on partitions with zerovalue, and
                                                # f2 will be applied on the result of partitions with zerovalue
# ---------------------------------------------------------------------------------------------------------------------

# aggregation
# rdd -> partitions
from sparkDrivers import createSparkDriver
spark = createSparkDriver()

l1 = [1, 2, 3, 4, 5, 6, 7, 8]
# this is how we can rdd with number of partition -- so r1 will have 3 partitions
r1 = spark.sparkContext.parallelize(l1, 3)
print(r1.getNumPartitions())
# This how you can perform a operation on a single partition.

def f1(x): yield list(x) # yield acts like return, it gets executed and forgot the data
r2 = r1.mapPartitions(lambda x:f1(x)) # to display map partitions
print(r2.collect()) # output -->  [[1, 2], [3, 4], [5, 6, 7, 8]]

# user only those type of operations on aggregations which can generate consistent result always
# reduce(lambda x,y: x-y) will not generate consistent result always

# 1,2 --> 1
# 3,4 --> 3
# 5,6,7,8 --> 5<6=>5, 5<7=>5, 5<8=>5

# fold --
# rdd = r1.fold(2,lambda x,y: x+y)
# 1,2 --> 2+1 -->3+2 = 5
# 3,4 --> 2+3=5 -->5+4 =9
# 5,6,7,8 --> 2+5=7 -->7+6=13 --> 13+7=20 -->20+8=28
#   5,9,28 --->2+5=7 --> 7+9=16 -->16+28= 44

# rd2 = rd1.fold(5000,lambda x,y:fun(x,y)) --> compare all the value with 5000 and will return minimum value using function 'fun'

# rd3 = rd.aggregate(2,(lambda x,y: x+y), (lambda x,y: x-y))
# print(rd3.collect()) --> Output will be: -40
# aggregate(zerovalue, fun1, fun2) --  here, zerovalue is 2
# fun1 will be applied on partitions data with zerovalue -- it is addition
# fun2 will be applied on the result of the partitions with zerovalue -- it is subtraction
# 1,2 --> 1+2=3--> 3+2=5
# 3,4 --> 2+3=5 --> 5+4=9
# 5,6,7,8 --> 2+5=7 -->7+6=13 -->13+7=20 --> 20+8=28
# so now we have 5, 9 and 28 and we will perform subtraction
# 5, 9, 28 --> 2-5=-3 -->-3-9=-12 -->-12-28=-40





