# -*- coding: utf-8 -*-
# @Time : 2022/9/12 12:08 
# @Author : chen.zhang 
# @File : 13_data_clear_api.py
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('text').master('local[*]').config('spark.sql.shuffle.partitions',
                                                                           2).getOrCreate()
    sc = spark.sparkContext

    """读取数据"""
    df = spark.read.format('csv').option('sep', ';').option('header', True).load('../data/input/sql/people.csv')

    # 数据清洗：数据去重
    # dropDuplicates 是DataFrame的API，可以完成数据去重
    # 无参数使用，对全部的列 联合起来进行比较，去除重复值，只保留一条
    df.dropDuplicates().show()

    df.dropDuplicates(['age', 'job']).show()

    # 数据清理：缺失值处理
    # dropna api是可以对缺失值的数据进行删除
    # 无参数使用，值要列中有null 就删除这一行数据
    df.dropna().show()

    # thresh =3 表示，最少满足3个有效列，不满足 就删除当前行数据
    df.dropna(thresh=3).show()

    df.dropna(thresh=2, subset=['name', 'age']).show()

    # 缺失值处理也可以完成对缺失值填充
    # DataFrame的fillna 对缺失列进行填充
    df.fillna('loss').show()

    df.fillna('N/A', subset=['job', 'age']).show()

    # 设定一个字典，对所有的列 提供填充规则
    df.fillna({'name': "未知姓名", "age": 1, "job": "worker"}).show()
