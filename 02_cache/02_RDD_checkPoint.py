# -*- coding: utf-8 -*-
# @Time : 2022/9/8 15:56 
# @Author : chen.zhang 
# @File : 01_RDD_cache.py
from pyspark import SparkConf, SparkContext
import os

os.environ['SPARK_HOME'] = '/opt/module/spark-yarn'
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    # 告知spark，开启CheckPoint功能
    sc.setCheckpointDir("hdfs://hadoop102:8020/wcoutput/ckp")

    rdd1 = sc.textFile('../data/input/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(' '))
    rdd3 = rdd2.map(lambda x: (x, 1))

    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

