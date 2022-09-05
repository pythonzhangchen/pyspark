# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:47 
# @Author : chen.zhang 
# @File : 03_RDD_create_wholeTextFile.py

from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取小文件文件夹
    rdd = sc.wholeTextFiles("../data/input/tiny_files")
    print(rdd.collect())
    print(rdd.map(lambda x:x[1]).collect())