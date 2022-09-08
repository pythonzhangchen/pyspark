# -*- coding: utf-8 -*-
# @Time : 2022/9/8 15:56 
# @Author : chen.zhang 
# @File : 01_RDD_cache.py
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile('../data/input/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(' '))
    rdd3 = rdd2.map(lambda x: (x, 1))

    rdd3.cache()
    rdd3.persist(storageLevel=StorageLevel.MEMORY_ONLY)

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    rdd3.unpersist()
