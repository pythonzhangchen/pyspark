# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:47 
# @Author : chen.zhang 
# @File : 01_RDD_create_parallelize.py
# 导入Spark的相关包
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化执行环境，构建SparkContext对象
    conf = SparkConf().setAppName("text").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 演示通过并行集合的方式创建RDD
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # parallelize方法,没有给定分区数，默认分区数是多少？根据CPU核心来顶
    print("默认分区数：", rdd.getNumPartitions())

    rdd = sc.parallelize([1, 2, 3], 3)
    print("分区数：", rdd.getNumPartitions())

    # collect方法，是RDD（分布式对象）中每个分区的数据，都发送到Driver中，形成一个Python List对象
    # collect： 分布式 转 → 本地集合
    print("rdd的内容是：", rdd.collect())