# -*- coding: utf-8 -*-
# @Time : 2022/9/18 17:19 
# @Author : chen.zhang 
# @File : 22_jdbc_spark_thrift_server.py
from pyhive import hive

if __name__ == '__main__':
    # 获取到Hive（Spark ThriftServer的连接）
    conn= hive.connect(host='192.168.10.102',port='10000', username='root')

    # 获取一个游标对象
    cursor =  conn.cursor()

    # 执行SQL
    cursor.execute("select * from student")

    # 通过fetchall API 获得返回值
    result = cursor.fetchall()
    print(result)