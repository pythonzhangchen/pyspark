# -*- coding: utf-8 -*-
# @Time : 2022/9/9 23:08 
# @Author : chen.zhang 
# @File : main.py
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from operator import add
from defs import context_jieba, append_words, filter_words, extract_user_word

import os

# os.environ['SPARK_HOME'] = '/opt/module/spark-yarn'
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local')
    # 集群模式不用指定master
    # conf = SparkConf().setAppName('test')
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile('../data/input/SogouQ.txt')
    # file_rdd = sc.textFile('hdfs://hadoop102:8020/wcinput/SogouQ.txt')

    # 对数据进行切分
    split_rdd = file_rdd.map(lambda x: x.split('\t'))

    # 因为要做多个需要，split_rdd最为基础RDD，会被多次使用

    split_rdd.persist(StorageLevel.DISK_ONLY)

    # TODO 需求1：用户搜索的关键’词‘分析
    # 主要分析热点此
    # 将所有的搜素内容取出
    # print(split_rdd.takeSample(True,3))
    content_rdd = split_rdd.map(lambda x: x[2])

    # 对搜索的内容进行分词分析
    words_rdd = content_rdd.flatMap(context_jieba)
    # print(words_rdd.collect())

    # 院校 帮→院校帮
    # 博学 谷→ 博学帮
    # 传智播 客→ 传智播客
    filtered_rdd = words_rdd.filter(filter_words)
    # 将关键词转换：传智播→传智播客
    final_words_rdd = filtered_rdd.map(append_words)
    result1 = final_words_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False,
                                                                     numPartitions=1).take(5)
    print("需求1结果：", result1)

    # TODO 用户和关键词组合分析
    # 1 我喜欢传智播客
    # 1+我 1+喜欢 1+传智播客
    user_content_rdd = split_rdd.map(lambda x: (x[1], x[2]))
    # 对用户的搜索内容进行分词，分词后和用户ID再次组合
    user_word_with_one = user_content_rdd.flatMap(extract_user_word)

    # 对内容进行 分组 聚合 排序 求前5
    result2 = user_word_with_one.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False,
                                                                        numPartitions=1).take(5)
    print("需求2结果：", result2)

    # TODO 需求3：热门搜索时间段分析
    # 取出来所有的时间
    time_rdd = split_rdd.map(lambda x: x[0])
    hour_with_one_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))
    result3 = hour_with_one_rdd.reduceByKey(add).sortBy(lambda x: x[1], ascending=False, numPartitions=1).collect()
    print("需求3结果：", result3)

    time.sleep(10000)