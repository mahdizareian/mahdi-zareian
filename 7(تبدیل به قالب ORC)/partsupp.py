from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/mahdi-parquet-data/partsupp.parquet")

partsupp.write.save("hdfs://namenode:8020/mahdi-orc-data/partsupp.orc", mode='overwrite', format='orc')

partsupp_orc = sqlContext.read.orc("hdfs://namenode:8020/mahdi-orc-data/partsupp.orc")
print(partsupp_orc.schema)
