# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:49 
# @Author : chen.zhang 
# @File : 13_operators_join.py
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(1001, '张三'), (1002, '李四'), (1003, '王五'), (1004, '赵六')])
    rdd2 = sc.parallelize([(1001, '销售部'), (1002, '科技部')])

    # 通过join算子来进行rdd之间关联
    # 对于join算子来说，按照二元元组的key来进行关联
    print(rdd1.join(rdd2).collect())

    # 左外连接
    print(rdd1.leftOuterJoin(rdd2).collect())