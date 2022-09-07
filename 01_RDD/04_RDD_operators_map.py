# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:47 
# @Author : chen.zhang 
# @File : 04_create_operators_map.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[8]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

    # 定义方法作为算子的传入函数体
    def add(data):
        return data*10

    print(rdd.map(add).collect())

    # 更简单的方式，是定义lambda表达式写匿名函数
    print(rdd.map(lambda data :data*10).collect())
    """
    对于算子的接收函数来说，两种方法都可以
    lambda表达式 适用于一行代码就搞定的函数体，如果是多行，需要定义独立的方法。
    """