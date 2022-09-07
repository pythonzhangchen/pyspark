# -*- coding: utf-8 -*-
# @Time : 2022/9/7 14:53 
# @Author : chen.zhang 
# @File : 22_operators_reduce.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1,10))

    print(rdd.reduce(lambda a, b: a + b))