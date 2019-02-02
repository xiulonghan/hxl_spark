# -*- coding: utf-8 -*-
# @author : hxl
# date:2019.01.31

from __future__ import print_function
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row


if __name__ == '__main__':
    path = 'F:\pycharmProjects\spark_learn\data'
    warehouse_location = abspath('spark-warehouse')
    print(warehouse_location)
    spark = SparkSession \
        .builder \
        .appName("sql_example") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    spark.sql("LOAD DATA LOCAL INPATH 'F:\pycharmProjects\spark_learn\data\kv1.txt' INTO TABLE src")

    # Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show()
