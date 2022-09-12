# -*- coding: utf-8 -*-
# @Time : 2022/9/12 16:34 
# @Author : chen.zhang 
# @File : 14_dataframe_write.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder.appName('text').master('local[*]').config('spark.sql.shuffle.partitions',
                                                                           2).getOrCreate()
    sc = spark.sparkContext

    # 构建一个rdd
    rdd = sc.parallelize([["hadoop spark flink"], ["hadoop flink java"]])
    df = rdd.toDF(["line"])


    # 注册UDF， UDF执行函数的定义
    def split_line(data):
        return data.split(" ")  # 返回值是一个Array对象


    # TODO 1 方式1 构建UDF
    udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))
    # DSL风格
    df.select(udf2(df['line'])).show()
    # SQL风格
    df.createTempView("lines")
    spark.sql("select udf1(line) from lines").show(truncate=False)

    # TODO 2 方式2的形式构建UDF
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df['line'])).show(truncate=False)
