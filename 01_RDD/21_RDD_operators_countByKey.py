# -*- coding: utf-8 -*-
# @Time : 2022/9/7 13:48 
# @Author : chen.zhang 
# @File : 21_operators_countByKey.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile('../data/input/words.txt')
    rdd2 = rdd.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1))
    # 通过countByKey来对key进行计算，这个一个Action样子
    print(rdd2.countByKey())