# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:49 
# @Author : chen.zhang 
# @File : 18_operators_sortByKey.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('E', 1), ('C', 1), ('D', 1), ('b', 1), ('g', 5), ('f', 1), ('Y', 1), ('u', 1), ('i', 1), ('o', 1), ('p', 1), ('m', 1), ('n', 1), ('j', 1), ('k', 1), ('l', 1)],3)

    print(rdd.sortByKey(ascending=True, numPartitions=3, keyfunc=lambda key: str(key).lower()).collect())