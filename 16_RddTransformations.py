# number of spark partitions is equal to number of input splits  (here number of splits are block if it is hdfs)
# blocks are derived from input splits

from sparkDrivers import createSparkDriver
spark = createSparkDriver()

r1 = spark.sparkContext.parallelize([('a',10),('b',5),('c',15),('d',12),('a',10),('b',30)])
r2 = spark.sparkContext.parallelize([('a',50),('b',15),('d',15),('e',12),('c',10),('a',30)])
r3 = spark.sparkContext.parallelize([20,30,40,50,60])
# print(r3.max())
# print(r3.min())
# print(r3.mean())
# print(r3.variance())

# print(r1.collect())
# print(r1.keys().collect())
# print("distinct",r1.keys().distinct().collect())
# print("without distinct values",r1.values().collect()) # output: ('without distinct values', [10, 5, 15, 12, 10, 30])
# print("values",r1.values().distinct().collect()) # output ('values', [10, 12, 5, 30, 15])

# joints can be performed on rdd and it use keys to perform join operations by default it will be Inner Join
# leftOuterJoin, rightOuterJoin,fullOuter
# j1 = r1.join(r2) #join
# print(j1.collect()) # output: [('a', (10, 50)), ('a', (10, 30)), ('a', (10, 50)), ('a', (10, 30)), ('c', (15, 10)), ('b', (5, 15)), ('b', (30, 15)), ('d', (12, 15))]
# j2 = r1.leftOuterJoin(r2)
# print(j2.collect())

x1 = spark.sparkContext.parallelize([(1,2,3),(2,3,4),(3,4,5),(4,5,6)]) # <-- this is not pair rdd
x2 = spark.sparkContext.parallelize([(10,20),(20,30),(30,40)])

# d1 = spark.sparkContext.parallelize([('a',(10,20,30,40,50)),('b',(1,2,3))])
# temp = d1.flatMap(lambda x:x)
# d1 = spark.sparkContext.parallelize([(1,2,40),(2,3),(3,4,20)])
# temp2 = d1.flatMapValues(lambda x:x)
# print(temp2.collect()) # output: [('a', 10), ('a', 20), ('a', 30), ('a', 40), ('a', 50), ('b', 1), ('b', 2), ('b', 3)]
# temp3 = d1.map(lambda x:(x[0],len(x[1])))
# print(temp3.collect()) #output: [('a', 5), ('b', 3)]
# temp4 = d1.mapValues(lambda x:len(x)) # here x represent just values
# print(temp4.collect()) #output: [('a', 5), ('b', 3)]

d1 = spark.sparkContext.parallelize([(101,'shankar','betech'),(102,'kumar','bsc')])
# d2 = d1.map(lambda x:(x[0],(x[1],x[2])))
# print(d2.collect()) #output: [(101, ('shankar', 'betech')), (102, ('kumar', 'bsc'))]
# d2 = d1.keyBy(lambda x:x[0]) # it will separate first element of the list and make it a key
# print(d2.collect()) # output: [(101, (101, 'shankar', 'betech')), (102, (102, 'kumar', 'bsc'))]

e1 = spark.sparkContext.textFile("D:\PythonAndSparkforBigDataMaster\lordofthering.txt")
print(e1.getNumPartitions())
print(d1.getNumPartitions())
c1 = e1.coalesce(2)
print('hey there' , c1.getNumPartitions())
# set operations
# repartition(n) = to partitions data and it involves shuffling of data
# increasing partitions will have shuffling - it compulsory  - repartitions can be used - to increasing coalesce can't be used
# decreasing - repartition, coalesce can be used
# coalesce and repartition - repartition require shuffling, coalesce - used to save data and coalesce is less costly
# intersection - combine just common data, union - combine all data
union = e1.union(c1)
print(union.getNumPartitions())
intersection = e1.intersection(c1)
print(intersection.getNumPartitions())
cart = e1.cartesian(c1) # cartesian is a*b = {(1,1),(1,2),(1,3),(2,1),.... } no comparison required