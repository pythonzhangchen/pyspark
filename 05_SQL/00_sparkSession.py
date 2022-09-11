# -*- coding: utf-8 -*-
# @Time : 2022/9/11 12:51 
# @Author : chen.zhang 
# @File : 00_sparkSession.py

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder.appName("test").master('local[*]').getOrCreate()
    # 通过SparkSession对象 获取 SparkContext对象
    sc = spark.sparkContext

    # SparkSQL的HelloWord
    df = spark.read.csv('../data/input/stu_score.txt', sep=',', header=False)
    df2 = df.toDF('id', 'name', 'score')
    df2.printSchema()
    df2.show()

    df2.createTempView('score')

    # SQL风格
    spark.sql("""select * from score where name='语文' limit 5""").show()

    # DSL风格
    df2.where('name="数学"').limit(5).show()
