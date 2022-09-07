# -*- coding: utf-8 -*-
# @Time : 2022/9/7 15:00 
# @Author : chen.zhang 
# @File : 23_operators_flod.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 3, 1, 5, 3, 2, 6], 1)

    print(rdd.takeSample(False, 5, 1))
