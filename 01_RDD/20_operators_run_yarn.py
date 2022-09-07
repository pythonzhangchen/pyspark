import json
import os

from pyspark import SparkConf, SparkContext
from defs_21 import city_with_category

os.environ['SPARK_HOME'] = '/opt/module/spark-yarn'
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

if __name__ == '__main__':
    conf = SparkConf().setAppName("test-yarn-1").setMaster("yarn")
    # 如果提交道集群运行，除了主代码以外，还需要依赖其它的代码文件
    # 需要设置一个参数，来告知spark，还有依赖文件要同步上传道集群中
    # 参数叫做：spark.submit.pyFiles
    # 参数的值可以是 单个.py文件， 也可以是.zip压缩包（有多个依赖文件的时候可以用zip压缩后上传）
    conf.set("spark.submit.pyFiles", "defs_21.py")
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile("hdfs://hadoop102:8020/wcinput/order.text")

    # 进行rdd数据的split 按照|符号进行 得到一个个json数据
    jsons_rdd = file_rdd.flatMap(lambda x: x.split("|"))

    # 通过Python内置的json库，完成json字符串到字典对象的转换
    dict_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))

    # 过滤数据，只保留北京的数据
    beijing_rdd = dict_rdd.filter(lambda d: d['areaName'] == '北京')

    # 组合北京和商品类型形成新的字符串
    category_rdd = beijing_rdd.map(city_with_category)

    # 对结果进行去重操作
    result_rdd = category_rdd.distinct()
    # 输出
    print(result_rdd.collect())


