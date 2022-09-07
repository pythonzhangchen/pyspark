# -*- coding: utf-8 -*-
# @Time : 2022/9/5 23:49 
# @Author : chen.zhang 
# @File : 19_operators_demo.py
import json

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile("../data/input/order.text")

    # 进行rdd数据的split 按照|符号进行 得到一个个json数据

    jsons_rdd = file_rdd.flatMap(lambda x: x.split("|"))

    # 通过Python内置的json库，完成json字符串到字典对象的转换
    dict_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))

    # 过滤数据，只保留北京的数据
    beijing_rdd = dict_rdd.filter(lambda d: d['areaName'] == '北京')

    # # 组合北京和商品类型形成新的字符串
    category_rdd = beijing_rdd.map(lambda x: x['areaName'] + '_' + x['category'])
    #
    # # 对结果进行去重操作
    result_rdd = category_rdd.distinct()
    # 输出
    print(result_rdd.collect())

