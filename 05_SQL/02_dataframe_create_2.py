# -*- coding: utf-8 -*-
# @Time : 2022/9/11 13:16 
# @Author : chen.zhang 
# @File : 01_dataframe_create_1.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.appName('test').master('local[8]').getOrCreate()
    sc = spark.sparkContext

    # 基础RDD构建DataFrame
    rdd = sc.textFile("../data/input/sql/people.txt").map(lambda x: x.split(',')).map(lambda x: [x[0], int(x[1])])

    # 构建表结构的描述对象
    schema = StructType().add("name", StringType(), nullable=True).add("age", StringType(), nullable=True)

    # 基于StructType对象去构建RDD为DF对象
    df = spark.createDataFrame(rdd, schema=schema)
    df.printSchema()
    df.show()
