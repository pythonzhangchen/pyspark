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

    # toDF的方式构架DataFrame
    df1 = rdd.toDF(['name', 'age'])
    df1.printSchema()
    df1.show()

    # toDF的方式2 通过StructType来构建
    schema = StructType().add('name', StringType(), nullable=True).add('age', IntegerType(), nullable=False)
    df2 = rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()
