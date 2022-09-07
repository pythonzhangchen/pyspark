# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:49 
# @Author : chen.zhang 
# @File : 15_operators_glom.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    print(rdd.glom().flatMap(lambda x: x).collect())