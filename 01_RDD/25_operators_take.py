# -*- coding: utf-8 -*-
# @Time : 2022/9/7 15:00 
# @Author : chen.zhang 
# @File : 23_operators_flod.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])

    print(rdd.take(5))
