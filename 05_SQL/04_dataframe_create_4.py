# -*- coding: utf-8 -*-
# @Time : 2022/9/11 13:16 
# @Author : chen.zhang 
# @File : 01_dataframe_create_1.py
from pyspark.sql import SparkSession
import pandas as pd

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.appName('test').master('local[8]').getOrCreate()
    sc = spark.sparkContext

    # 基础RDD构建DataFrame
    rdd = sc.textFile("../data/input/sql/people.txt").map(lambda x: x.split(',')).map(lambda x: [x[0], int(x[1])])

    # 基于pandas的DataFrame构建SparkSession的DataFrame对象
    pdf = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['张大仙', '王晓晓', '吕不韦'],
        'age': [11, 21, 11]

    })
    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()
