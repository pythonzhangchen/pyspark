# -*- coding: utf-8 -*-
# @Time : 2022/9/11 18:18 
# @Author : chen.zhang 
# @File : 11_worldcount_demo.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    # TODO SQL风格进行处理
    rdd = sc.textFile('../data/input/words.txt').flatMap(lambda x: x.split(' ')).map(lambda x: [x])
    df = rdd.toDF(['word'])

    # 注册DF为表格
    df.createTempView("words")

    spark.sql("select word,count(*) as cnt from words group by word").show()

    # TODO DSL 风格处理
    df = spark.read.format('text').load('../data/input/words.txt')

    # withColumn 方法
    # 方法功能：对已存在的列进行操作，返回一个新的列，如果名字和老列相同，那么替换，否则作为新列存在
    df2 = df.withColumn("value", F.explode(F.split(df['value'], ' ')))
    df2.groupBy('value').count().withColumnRenamed('value', 'word').withColumnRenamed('count', 'cnt').orderBy('cnt',
                                                                                                              ascending=False).show()
