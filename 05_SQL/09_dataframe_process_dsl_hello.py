# -*- coding: utf-8 -*-
# @Time : 2022/9/11 13:16 
# @Author : chen.zhang 
# @File : 01_dataframe_create_1.py
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").schema('id INT,subject STRING,score INT').load('../data/input/stu_score.txt')

    # Column对象的获取
    id_column = df['id']
    subject_column = df['subject']

    # DLS风格演示
    df.select(['id', 'subject']).show()
    df.select('id', 'subject').show()
    df.select(id_column, subject_column).show(15, False)
    # filter API
    df.filter("score<99").show()
    df.filter(df['score'] < 99).show()

    # where API
    df.where("score<99").show()
    df.where(df['score'] < 99).show()

    # group by API
    df.groupBy('subject').count().show()
    df.groupBy(df['subject']).count().show()

    # df.groupBy API的返回值，GroupData
    # GroupData对象 不是DataFrame
    # 它是一个 有分组关系的数据结构，有一些API提供我们对分组做聚合
    # SQL：group by 后接上聚合：sum avg count min max
    # GroupData 类似于SQL分组后的数据结构，同样有上述5中聚合方法
    # GroupData调用聚合方法后返回值是DataFrame
    # GroupData对象只是一个中专的对象，最终还是药获得DataFrame的结果
    r = df.groupBy('subject')
    r.count().show()
