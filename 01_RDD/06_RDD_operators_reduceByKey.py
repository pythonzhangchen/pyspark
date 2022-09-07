# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:48 
# @Author : chen.zhang 
# @File : 06_rdd_operators_reduceByKey.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[8]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('a', 1)])
    print(rdd.reduceByKey(lambda a, b: a + b).collect())
