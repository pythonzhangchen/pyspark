# -*- coding: utf-8 -*-
# @Time : 2022/9/11 18:18 
# @Author : chen.zhang 
# @File : 11_worldcount_demo.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    schema = StructType(). \
        add('user_id', StringType(), nullable=True). \
        add('movie_id', IntegerType(), nullable=True). \
        add('rank', IntegerType(), nullable=True). \
        add('ts', StringType(), nullable=True)

    df = spark.read.format('csv').option('sep', '\t').option('header', False).option('encoding', 'utf-8').schema(
        schema).load('../data/input/sql/u.data')

    # TODO  1 用户平均分
    df.groupBy('user_id').avg('rank').withColumnRenamed('avg(rank)', 'avg_rank').withColumn('avg_rank',
                                                                                            F.round('avg_rank',
                                                                                                    2)).orderBy(
        'avg_rank', ascending=False).show()

    # TODO 2 电影的平均分查询
    df.createTempView('movie')
    spark.sql(
        'select movie_id,round(avg(rank),2) as avg_rank from movie group by movie_id order by avg_rank desc').show()

    # TODO 3 查询大于平均分的电影的数量 # Row
    # print('大于平均分的电影的数量:', df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())
    spark.sql(
    'select count(*) from movie where rank>(select avg(rank) from movie)').show()
    # TODO 4 查询高分电影中（>3）打分次数最多的用户，此人打分的平均数
    user_id = \
        df.where('rank> 3').groupBy('user_id').count().withColumnRenamed('count', 'cnt').orderBy('cnt',
                                                                                                 ascending=False).limit(
            1).first()['user_id']
    # 计算这个人的打分平均分
    df.filter(df['user_id'] == user_id).select(F.round(F.avg('rank'), 2)).show()

    # TODO 5 查询每个用户的平均打分，最低打分，最高打分
    df.groupBy('user_id').agg(
        F.round(F.avg('rank'), 2).alias("avg_rank"),
        F.min("rank").alias("min_rank"),
        F.max("rank").alias("max_rank")
    ).show()

    # TODO 6 查询平分超过100次的电影，的平均分 排名TOP100

    df.groupBy('movie_id').agg(
        F.count('movie_id').alias('cnt'),
        F.round(F.avg('rank'),2).alias('avg_rank')
    ).filter('cnt>100').orderBy('avg_rank',ascending=False).limit(10).show()

    """
    1、 agg：它是GroupData对象的API，作用是 在里面可以写多个聚合
    2、 alias：他是Column对象的API，可以针对一个列 进行改名
    3、 withColumnRenamed：它是GroupData对象的API，可以对DF中的列进行改名，一次改一个列， 可以链式调用
    4、 orderBy：DataFrame的API，取出DF的第一行数据，返回值结果是ROW对象
    5、 first：DataFrame的API，取出DF的第一行数据，返回值结果是ROW对象
    # Row对象 就是一个数组，你可以通过row[’列名‘]来取出当前行中，某一列的具体数值，返回住不再是DF 或者GroupData 或者Column里面具体的值（数字或字符串等）
    """