from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/mahdi-parquet-data/lineitem.parquet")

lineitem.write.save("hdfs://namenode:8020/mahdi-orc-data/lineitem.orc", mode='overwrite', format='orc')

lineitem_orc = sqlContext.read.orc("hdfs://namenode:8020/mahdi-orc-data/lineitem.orc")
print(lineitem_orc.schema)
