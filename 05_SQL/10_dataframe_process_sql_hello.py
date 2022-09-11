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

    # 注册临时表
    df.createTempView("score") # 注册临时视图
    df.createOrReplaceTempView("score_2") # 注册或者替换临时视图
    df.createGlobalTempView("score_3") # 注册全局临时视图，全局临时视图在使用的时候 需要在前面带上global_temp.前缀

    # 可以通过SparkSession对象的sql api来完成sql语句的执行
    spark.sql("select subject,count(*) as cnt from score group  by subject").show()
    spark.sql("select subject,count(*) as cnt from score_2 group  by subject").show()
    spark.sql("select subject,count(*) as cnt from global_temp.score_3 group  by subject").show()