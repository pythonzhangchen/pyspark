# -*- coding: utf-8 -*-
# @Time : 2022/9/11 13:16 
# @Author : chen.zhang 
# @File : 01_dataframe_create_1.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    # 构建StructType,text数据源，读取数据的特点是，将一整行只作为一个列读取，默认列名是value 类型是String
    schema = StructType().add("data", StringType(), nullable=True)
    df = spark.read.format("text").schema(schema).load("../data/input/sql/people.txt")
    df.printSchema()
    df.show()
