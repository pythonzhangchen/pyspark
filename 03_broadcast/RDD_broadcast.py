# -*- coding: utf-8 -*-
# @Time : 2022/9/10 14:20 
# @Author : chen.zhang 
# @File : test.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('test')
    sc = SparkContext(conf=conf)

    stu_info_list = [(1, '张大仙', 11), (2, '王晓晓', 13), (3, '张甜甜', 11), (4, '王大力', 11)]

    score_info_rdd = sc.parallelize([(1, '语文', 99), (2, '数学', 99), (3, '英语', 99), (4, '编程', 99)])

    # 将本地对象标记为广播变量
    broadcast = sc.broadcast(stu_info_list)

    def map_func(data):
        name=''
        id = data[0]
        # 匹配本地list和分布式rdd中的学生ID 匹配成功后 即可获得当前学生的姓名
        for stu_info in broadcast.value:
            if id == stu_info[0]:
                name = stu_info[1]
        return (name, data[1], data[2])


    print(score_info_rdd.map(map_func).collect())
"""
场景1：本地集合对象和分布式集合对象（RDD）进行关联的时候
需要将本地集合对象 封装为广播变量
可以节省：
1、网络IO的次数
2、Executor的内存占用
"""