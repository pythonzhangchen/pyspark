# -*- coding: utf-8 -*-
# @Time : 2022/9/3 19:15 
# @Author : chen.zhang 
# @File : HelloWorld.py

from pyspark import SparkConf, SparkContext
import os

os.environ['SPARK_HOME'] = '/opt/module/spark-yarn'
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

# os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jre1.8.0_281'

if __name__ == '__main__':
    # conf = SparkConf().setMaster("local[*]").setAppName("WorldCountHelloWorld")
    conf = SparkConf().setMaster("yarn").setAppName("WorldCountHelloWorld")
    # 通过SparkConf 对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求：WordCount 单词计算，读取HDFS上的word.txt，对其内部的单词统计出现的数据
    # 读取文件
    file_rdd = sc.textFile("hdfs://hadoop102:8020/wcinput/hello.txt")
    # file_rdd = sc.textFile("../data/input/words.txt")
    # 将单词进行切割，得到一个存储全部单词的集合对象
    word_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    # 将单词转换为元组对象，key是单词，value是数字1
    words_with_one_ree = word_rdd.map(lambda x: (x, 1))
    # 将元组的value 按照key来分组，对所有的value执行聚合操作（相加）
    result_rdd = words_with_one_ree.reduceByKey(lambda a, b: a + b)
    # 通过collect方法收集RDD的数据打印输出结果
    print(result_rdd.collect())
