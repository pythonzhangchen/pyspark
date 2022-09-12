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

    rdd = sc.parallelize([
        ('张三', 'class_1', 99),
        ('王五', 'class_2', 35),
        ('王三', 'class_3', 57),
        ('王九', 'class_4', 12),
        ('王丽', 'class_5', 99),
        ('王娟', 'class_1', 90),
        ('王军', 'class_2', 91),
        ('王俊', 'class_3', 33),
        ('王君', 'class_4', 55),
        ('王均', 'class_5', 66),
        ('郑颖', 'class_1', 11),
        ('郑辉', 'class_2', 33),
        ('张丽', 'class_3', 36),
        ('张张', 'class_4', 79),
        ('黄凯', 'class_5', 90),
        ('黄开', 'class_1', 11),
        ('黄恺', 'class_2', 11),
        ('王凯', 'class_3', 3),
        ('王世杰', 'class_1', 99),
        ('王凯杰', 'class_2', 90),
        ('王开杰', 'class_3', 3),
        ('王景亮', 'class_1', 11)])
    schema = StructType().add('name', StringType()).add('class', StringType()).add('score', IntegerType())
    df = rdd.toDF(schema)

    df.createTempView('stu')

    # TODO 聚合窗口函数的演示
    spark.sql("select *, avg(score) over() as avg_score from stu").show()

    # TODO 排序相关的窗口函数计算
    # RANK over DENSE_RANK over  ROW_NUMBER over
    spark.sql(
        'select *,row_number() over(order by score desc) as row_number_rank,DENSE_RANK() over(partition by class order by score desc) as dense_rank,rank() over(order by score) as rank from stu').show()

    # TODO NTILE
    spark.sql('select *, NTILE(6) over(order by score desc) from stu').show()