from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')
customer = sqlContext.read.parquet("hdfs://namenode:8020/mahdi-parquet-data/customer.parquet")

customer.write.save("hdfs://namenode:8020/mahdi-orc-data/customer.orc", mode='overwrite', format='orc')

customer_orc = sqlContext.read.orc("hdfs://namenode:8020/mahdi-orc-data/customer.orc")
print(customer_orc.schema)
