# -*- coding: utf-8 -*-
# @Time : 2022/9/7 17:28 
# @Author : chen.zhang 
# @File : 32_RDD_operatord_mapPartitions.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)


    def process(iter):
        result = []
        for it in iter:
            result.append(it * 10)
        return result
        # print(result)


    rdd.foreachPartition(process)
