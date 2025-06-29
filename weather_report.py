


!pip install pyspark
from pyspark.sql import SparkSession
from google.colab import files
uploaded=files.upload()
spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()

df = spark.read.parquet("weather.parquet")
df.show(5)
df.coalesce(1).write.csv("output_folder", header=True)

spark.stop()

import shutil
from google.colab import files
output_folder ="/content/output_folder"
shutil.make_archive("output_folder",'zip',output_folder)
files.download("output_folder"+".zip")