from pyspark.sql import SparkSession

def createSparkDriver():
    sparkdriver = SparkSession.builder.master('local').appName('app').getOrCreate()
    return sparkdriver