# coding: utf-8
# @Time : 2022/9/18 13:02 
# @Author : chen.zhang 
# @File : 21_spark_on_hive.py
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    # enableHiveSupport 开启对hive的支持
    spark = SparkSession.builder.\
        appName("test").\
        master('local[*]').\
        config('spark.sql.shuffle.partitions', 2). \
        config('spark.sql.warehouse.dir', 'hdfs://hadoop102:8020/user/hive/warehouse'). \
        config('hive.metastore.uris', 'thrift://hadoop102:9083').\
        enableHiveSupport().\
        getOrCreate()

    spark.sql('select * from student').show()
