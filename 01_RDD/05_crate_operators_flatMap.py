# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:48 
# @Author : chen.zhang 
# @File : 05_crate_operators_flatMap.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(['hadoop spark hadoop','spark hadoop hadoop','hadoop flink spark'])
    rdd2 = rdd.flatMap(lambda line:line.split(" "))
    print(rdd2.collect())