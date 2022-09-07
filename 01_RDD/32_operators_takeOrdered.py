# -*- coding: utf-8 -*-
# @Time : 2022/9/7 15:00 
# @Author : chen.zhang 
# @File : 23_operators_flod.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6])
    print(rdd.takeOrdered(3))
    print(rdd.takeOrdered(3, lambda x: -x))
