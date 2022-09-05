# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:49 
# @Author : chen.zhang 
# @File : 16_operators_groupByKey.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])
    print(rdd.groupByKey().map(lambda x: [x[0], list(x[1])]).collect())