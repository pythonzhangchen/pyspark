# -*- coding: utf-8 -*-
# @Time : 2022/9/10 17:10 
# @Author : chen.zhang 
# @File : RDD_broadcast_and_accumulator_demo.py
from pyspark import SparkConf, SparkContext
import re

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('test')
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile('../data/input/accumulator_broadcast_data.txt')

    # 特殊字符的list定义
    abnormal_char = [',', '.', '!', '#', '$', '%']

    # 将特殊字符list 包装或广播变量
    broadcast = sc.broadcast(abnormal_char)

    # 对特殊字符出现次数做累加，累加使用累加器最好
    acmlt = sc.accumulator(0)

    # 数据处理，先处理数据的空行,再python中有内容就是True None就是False
    line_rdd = file_rdd.filter(lambda line: line.strip())

    # 去除前后空格
    data_rdd = line_rdd.map(lambda line: line.strip())

    # 对数据进行切分，按照正则表达式切分，因为空格分隔符某些之间是两个或多个空格
    words_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))


    # 当前words_rdd中有正常单词，也有特殊符号
    # 现在在需要过滤数据，保证正常单词用于做单词计数，再过滤 的过程中 对特殊符号做计数
    def filter_func(data):
        global acmlt
        # 取出广播变量中存储的list
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            # 表示这个是 特殊字符
            acmlt += 1
            return False
        else:
            return True


    abnormal_words_rdd = words_rdd.filter(filter_func)

    # 把正常单词的单词计数逻辑
    result_rdd = abnormal_words_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    print("正常单词计数结果：", result_rdd.collect())
    print("特殊字符数量：", acmlt)
