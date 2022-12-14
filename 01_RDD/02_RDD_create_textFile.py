# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:47 
# @Author : chen.zhang 
# @File : 02_RDD_create_textFile.py

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 通过textFile API 读取数据

    # 读取本地文件数据
    file_rdd1 = sc.textFile("../data/input/words.txt")
    print("默认读取分区数：", file_rdd1.getNumPartitions())
    print("file_rdd1 内容：", file_rdd1.collect())

    # 最小分区数参数测试
    file_rdd2 = sc.textFile("../data/input/words.txt", 3)
    # 最小分区数参考值，Spark有自己的判断，你给的太大Spark不会理会
    file_rdd3 = sc.textFile("../data/input/words.txt", 100)
    print("file_rdd2 分区数：", file_rdd2.getNumPartitions())
    print("file_rdd3 分区数：", file_rdd3.getNumPartitions())