from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')
part = sqlContext.read.parquet("hdfs://namenode:8020/mahdi-parquet-data/part.parquet")

part.write.save("hdfs://namenode:8020/mahdi-orc-data/part.orc", mode='overwrite', format='orc')

part_orc = sqlContext.read.orc("hdfs://namenode:8020/mahdi-orc-data/part.orc")
print(part_orc.schema)
