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

    # 读取csv类型的文件
    df = spark.read.format("csv").option('sep', ';').option('header', True).option('encoding', 'utf-8').schema(
        'name STRING,age INT,job STRING').load('../data/input/sql/people.csv')
    df.printSchema()
    df.show()
