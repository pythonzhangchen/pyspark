# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:49 
# @Author : chen.zhang 
# @File : 12_operators_union.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 1, 2, 2, ])

    rdd2 = sc.parallelize(['a', 'b', 'c'])

    rdd3 = sc.parallelize([('a', 1), ('b', 2), ('c', 3)])
    print(rdd.union(rdd2).union(rdd3).collect())

"""
1、 可以看到union算子不会去重的
2、RDD的类型不同也是可以合并的
"""