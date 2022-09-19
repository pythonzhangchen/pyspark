# coding:utf8

'''
需求1：各省市销售额统计
需求2：Top3销售省份中，有多少店铺达到过日销售额1000+
需求3：Top3省份中，歌声的平均单单价
需求4：Top3省份中，各个省份的支付类型比例

receivable: 订单金额
storeProvince：店铺省份
dateTs：订单的销售日期
payType：支付类型
storeID：店铺ID

2 操作
1. 写入结果到Mysql
2. 写入结果到hive
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType
import os

os.environ['SPARK_HOME'] = '/opt/module/spark-yarn'
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("Spaek Example"). \
        master("yarn"). \
        config("spark.sql.shuffle.partition", 3). \
        config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://hadoop102:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    # 1、读取数据
    # 省份信息，缺失值过滤,同时省份信息中，会有“null” 字符串
    # 订单的金额, 数据集中有的订单的金额是单笔超过10000的，这些是测试数据
    # 列值裁剪（Spark会自动做这个优化）
    df = spark.read.format("json").load("../data/input/mini.json"). \
        dropna(thresh=1, subset=['storeProvince']). \
        filter("storeProvince != 'null'"). \
        filter("receivable<10000"). \
        select("storeProvince", "receivable", "dateTS", "payType", "storeID")

    # TODO 需求1： 各省销售额统计
    province_sale_df = df.groupBy("storeProvince"). \
        sum("receivable"). \
        withColumnRenamed("sum(receivable)", "money"). \
        withColumn("money", F.round("money", 2)). \
        orderBy("money", ascending=False)
    province_sale_df.show()
    # 写出Mysql
    province_sale_df.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://hadoop102:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8"). \
        option("dbtable", "province_sale"). \
        option("user", "root"). \
        option("password", "000000"). \
        option("encoding", "utf-8"). \
        save()

    # 写出Hive表 saveAsTable可以写出表，要求已经配置好spark on hive,配置好后
    # 会将表写入到Hive的数据仓库中
    province_sale_df.write.mode("overwrite").saveAsTable("default.province_sale", "parquet")

    # TODO 需求2 Top3销售省份中，有多少店铺达到过日销售额1000+
    top3_province_df = province_sale_df.limit(3).select('storeProvince').withColumnRenamed('storeProvince',
                                                                                           'top3_storeProvince')

    # 2.2 和原始的DF进行关联
    top3_province_df_joined = df.join(top3_province_df,
                                      on=df['storeProvince'] == top3_province_df['top3_storeProvince'])

    # 后面还需要使用加入到缓存
    top3_province_df_joined.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

    province_hot_store_count_df = top3_province_df_joined.groupBy('storeProvince', 'storeID',
                                                                  F.from_unixtime(df['dateTs'].substr(0, 10),
                                                                                  'yyyy-MM-dd').alias("day")). \
        sum('receivable').withColumnRenamed("sum(receivable)", "money"). \
        filter("money>1000").dropDuplicates(subset=['storeID']). \
        dropDuplicates(subset=['storeID']). \
        groupBy("storeProvince"). \
        count()

    province_hot_store_count_df.show()

    # 写出Mysql
    province_hot_store_count_df.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://hadoop102:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8"). \
        option("dbtable", "province_hot_store_count"). \
        option("user", "root"). \
        option("password", "000000"). \
        option("encoding", "utf-8"). \
        save()

    # 会将表写入到Hive的数据仓库中
    province_hot_store_count_df.write.mode("overwrite").saveAsTable("default.province_hot_store_count", "parquet")

    # TODO 需求3：Top3省份中，各省的平均单单价
    top3_province_order_avg_df = top3_province_df_joined.groupBy('storeProvince'). \
        avg('receivable'). \
        withColumnRenamed('avg(receivable)', 'money'). \
        withColumn('money', F.round('money', 2)). \
        orderBy('money', ascending=False)

    top3_province_order_avg_df.show()

    # 写出Mysql
    top3_province_order_avg_df.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://hadoop102:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8"). \
        option("dbtable", "top3_province_order_avg"). \
        option("user", "root"). \
        option("password", "000000"). \
        option("encoding", "utf-8"). \
        save()

    # 会将表写入到Hive的数据仓库中
    top3_province_order_avg_df.write.mode("overwrite").saveAsTable("default.top3_province_order_avg", "parquet")

    # TODO 需求4：Top3省份中，各个省份的支付类型比例
    top3_province_df_joined.createTempView("province_pay")


    def udf_func(percent):
        return str(round(percent * 100, 2)) + '%'


    # 注册UDF
    my_udf = F.udf(udf_func, StringType())

    patType_df = spark.sql("""
    select storeProvince,payType,(count(payType)/total) as percent from 
    (select storeProvince,payType,count(1) over(partition by storeProvince) AS total from province_pay) as sub
    group by storeProvince,payType,total""").withColumn("percent", my_udf("percent"))
    patType_df.show()
    # 写出Mysql
    patType_df.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://hadoop102:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8"). \
        option("dbtable", "pay_type"). \
        option("user", "root"). \
        option("password", "000000"). \
        option("encoding", "utf-8"). \
        save()

    # 会将表写入到Hive的数据仓库中
    patType_df.write.mode("overwrite").saveAsTable("default.pay_type", "parquet")
    top3_province_df_joined.unpersist()
