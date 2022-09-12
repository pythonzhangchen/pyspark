# -*- coding: utf-8 -*-
# @Time : 2022/9/12 16:34 
# @Author : chen.zhang 
# @File : 14_dataframe_write.py
import string

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder.appName('text').master('local[*]').config('spark.sql.shuffle.partitions',
                                                                           2).getOrCreate()
    sc = spark.sparkContext

    # 假设有三个数字 1 2 3 我们传入数字，返回数据所在序号对应的 字母 然后和数字结合形成dict返回
    # 比如传入1 我们返回{'num':1,'letyers':'Q'}
    rdd = sc.parallelize([[1], [2], [3]])
    df = rdd.toDF(['num'])


    # 注册UDF
    def process(data):
        return {"num": data, "letters": string.ascii_letters[data]}


    """
    UDF的返回值是字典，需要用StructType来接收
    """
    udf1 = spark.udf.register('udf1', process, StructType().add('num', IntegerType(), nullable=True).add('letters',
                                                                                                         StringType(),
                                                                                                         nullable=True))
    df.selectExpr("udf1(num)").show(truncate=False)
    df.select(udf1(df['num'])).show(truncate=False)
