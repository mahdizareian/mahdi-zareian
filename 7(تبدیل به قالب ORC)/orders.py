from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')
orders = sqlContext.read.parquet("hdfs://namenode:8020/mahdi-parquet-data/orders.parquet")

orders.write.save("hdfs://namenode:8020/mahdi-orc-data/orders.orc", mode='overwrite', format='orc')

orders_orc = sqlContext.read.orc("hdfs://namenode:8020/mahdi-orc-data/orders.orc")
print(orders_orc.schema)
