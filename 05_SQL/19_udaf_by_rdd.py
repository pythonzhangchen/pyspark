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


    rdd = sc.parallelize([1, 2, 3, 4, 5],3)
    df = rdd.map(lambda x:[x]).toDF(['num'])

    # 折中的方式：使用mapPartitions 算子来完成聚合操作
    # 如果用使用mapPartitions API 完成UDAF聚合，一定要单分区
    single_partition_rdd= df.rdd.repartition(1)

    def process(iter):
        sum = 0
        for row in iter:
            sum +=row['num']
        return [sum] # 一定要桥套list，因为mapPartitons方法要求的返回值是list对象


    print(single_partition_rdd.mapPartitions(process).collect())
