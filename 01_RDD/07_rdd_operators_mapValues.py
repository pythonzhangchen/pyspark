# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:48 
# @Author : chen.zhang 
# @File : 07_rdd_operators_mapValues.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('a', 1)])
    print(rdd.mapValues(lambda x: x*10).collect())