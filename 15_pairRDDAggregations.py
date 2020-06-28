from sparkDrivers import createSparkDriver
spark = createSparkDriver()
# an rdd in which each record is  pair of elements (a,b) where a and b is elements such order is called pair rdd
# (a,b) -> by default spark consider 'a' as key and 'b' as value.
# ---------------------------------------------------------------------------------------------------------------------
# reduceByKey(f1)
# foldByKey(zerovalue,f1)
# aggregateByKey(zerovalue,f1,f2)
# combineByKey(zerovalue, f1,f2,f3)
# ---------------------------------------------------------------------------------------------------------------------

# l1 = [('a',10),('b',20),('c',15),('d',25),('a',30),('b',26),('c',10),('a',10),('d',10)]
# rd1 = spark.sparkContext.parallelize(l1,3)
#print(rd1.collect())

# def f1(x): yield list(x)
# rd2 = rd1.mapPartitions(lambda x:f1(x)) #mapPartitions() --> to display map partitions
# print(rd2.collect())

# r1 = rd1.countByKey()
# print(r1) # output: defaultdict(<type 'int'>, {'a': 3, 'c': 2, 'b': 2, 'd': 2})
# r2 = rd1.countByValue()
# print(r2) # output: defaultdict(<type 'int'>, {('d', 10): 1, ('b', 20): 1, ('c', 10): 1, ('d', 25): 1, ('a', 10): 2, ('c', 15): 1, ('a', 30): 1, ('b', 26): 1})
# r3 = rd1.sortByKey()
# print(r3.collect()) # output: [('a', 10), ('a', 30), ('a', 10), ('b', 20), ('b', 26), ('c', 15), ('c', 10), ('d', 25), ('d', 10)]
# two types of operations
# coarse grained --> bulk --> 50% hike to all employees will be in bulk
# fine grained --> 1 lakh customers   you cant update single record
# print(rd2.collect())

# s1= rd1.reduceByKey(lambda x, y: x+y).sortByKey() # all the key which has same name will be grouped first and then the function will be applied to the values of the grouped items
# print(s1.collect()) # output: [('b', 46), ('d', 35), ('a', 50), ('c', 25)]
# s2= rd1.reduceByKey(lambda x, y: x-y).sortByKey() # all the key which has same name will be grouped first and then the function will be applied to the values of the grouped items
# print(s2.collect()) # output: [('b', 46), ('d', 35), ('a', 50), ('c', 25)]


l1 = [('a',10),('b',20),('c',15),('a',25),('a',30),('b',26),('c',10),('a',10),('c',10)]
rd1 = spark.sparkContext.parallelize(l1,3)
def f1(x): yield list(x)
rd2 = rd1.mapPartitions(lambda x:f1(x))

s3 = rd1.foldByKey(2,lambda x,y: x+y)
print(s3.collect()) # output: [('b', 50), ('a', 81), ('c', 39)]
# ---foldByKey(2, lambda x,y: x+y)---
# check notebook
# zerovalue should be applied to each and every key elements in the partitions(just once)

s4 = rd1.aggregateByKey(3,(lambda x,y: x+y),(lambda x,y: x-y))
print(s4.collect()) # output: [('b', -6), ('a', -58), ('c', -5)]

s5 = rd1.groupByKey() # This is how it is going to group (a,[10,20,30,10]),(b,[20,26],(c,[15,10,10]))
# print(s5.collect()) #output:[('b', <pyspark.resultiterable.ResultIterable object at 0x000000000454F308>), ('a', <pyspark.resultiterable.ResultIterable object at 0x000000000454F4C8>), ('c', <pyspark.resultiterable.ResultIterable object at 0x000000000454F488>)]
s6 = s5.map(lambda x:(x[0],sum(x[1])))
print(s6.collect()) #output:[('b', 46), ('a', 75), ('c', 35)]


