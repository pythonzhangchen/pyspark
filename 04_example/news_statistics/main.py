# -*- coding: utf-8 -*-
# @Time : 2022/9/11 10:49 
# @Author : chen.zhang 
# @File : main.py
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName("news_test").setMaster('local[*]')
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile('../../data/input/apache.log')

    split_rdd = file_rdd.map(lambda x: x.split(' '))

    split_rdd.persist(StorageLevel.DISK_ONLY)

    # TODO 计算当前网站访问的PV（被访问次数）
    print('需求1结果：',
          split_rdd.map(lambda x: (x[4], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False,
                                                                                    numPartitions=1).collect())
    # TODO 当前访问的UV（访问的用户数）
    print('需求2结果：', split_rdd.map(lambda x: x[0]).distinct().count())

    # TODO 有那些IP访问了本网站
    print('需求2结果：', split_rdd.map(lambda x: x[0]).distinct().collect())

    # TODO 那个页面的访问量最高
    print('需求4结果：',
          split_rdd.map(lambda x: (x[4], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False,
                                                                                    numPartitions=1).take(1))
