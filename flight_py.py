

!pip install pyspark
from google.colab import files
uploaded=files.upload()
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Flight Timing").getOrCreate()
df=spark.read.parquet("/content/flights-1m.parquet")
df.printSchema()
df.show(5)
df.coalesce(1).write.csv("/content/flights-1m-output-csv",header=True, mode='overwrite')
spark.stop()
