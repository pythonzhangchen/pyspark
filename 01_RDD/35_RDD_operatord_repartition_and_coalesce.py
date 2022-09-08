# -*- coding: utf-8 -*-
# @Time : 2022/9/7 17:28 
# @Author : chen.zhang 
# @File : 32_RDD_operatord_mapPartitions.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd =sc.parallelize(range(1,6),3)
    # repartition 修改分区
    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())

    # coalesce 修改分区
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5,shuffle=True).getNumPartitions())
