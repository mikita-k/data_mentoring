from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]"). \
    appName('sandbox'). \
    getOrCreate()

result_df = spark.read.option("header", True).csv("result/*.csv.gz")
print("count is ", result_df.count())
result_df.show()
