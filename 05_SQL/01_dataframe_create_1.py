# -*- coding: utf-8 -*-
# @Time : 2022/9/11 13:16 
# @Author : chen.zhang 
# @File : 01_dataframe_create_1.py
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.appName('test').master('local[8]').getOrCreate()
    sc = spark.sparkContext

    # 基础RDD构建DataFrame
    rdd = sc.textFile("../data/input/sql/people.txt").map(lambda x: x.split(',')).map(lambda x: [x[0], int(x[1])])

    # 构建DataFrame对象
    # 参数1 被转换的RDD
    # 参数2 指定列名，通过list的形式指定，按照顺序一次提供字符串名称即可
    df = spark.createDataFrame(rdd, schema=['name', 'age'])

    # 打印DataFrame的表结构
    df.printSchema()

    # 打印df中的参数
    # 参数1 表示 展示出来多少条数据，默认不传的话是20
    # 参数2 表示是否对列进行截断，如果列的数据长度超过20个字符串长度，后续的内容不显示以...代替
    # 如果给False 表示不截断全部显示，默认是True
    df.show(20, False)

    # 将DF对象转换成临时视图表，可供sql语句查询
    df.createOrReplaceTempView('people')
    spark.sql("select * from people where age<30").show()