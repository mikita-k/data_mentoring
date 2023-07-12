from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min


def aggregation_distinct_hotels(df):
    result_df = df.groupBy("city").agg(count("address").alias("unique_hotels_count"))
    return result_df


def aggregation_average_temperature(df):
    result_df = df.groupBy("city").agg(avg("avg_tmpr_c").alias("average_temperature"))
    return result_df


def aggregation_max_temperature(df):
    result_df = df.groupBy("city").agg(max("avg_tmpr_c").alias("max_temperature"))
    return result_df


def aggregation_min_temperature(df):
    result_df = df.groupBy("city").agg(min("avg_tmpr_c").alias("min_temperature"))
    return result_df


# Create SparkSession
spark = SparkSession.builder.appName("StreamingExample").getOrCreate()

# Create StreamingContext with a 1-second interval
ssc = StreamingContext(SparkContext.getOrCreate(), 1)

# Read Parquet files
directory = "data/hotel-weather"
schema = spark.read.parquet(directory).schema
streaming_df = spark.readStream.format("parquet").schema(schema).load(directory)

# Filter data with null values
filtered_df = streaming_df.filter(col("address").isNotNull() & col("city").isNotNull() & col("avg_tmpr_c").isNotNull())

# Aggregation process
unique_hotels_df = aggregation_distinct_hotels(filtered_df)
average_temperature_df = aggregation_average_temperature(filtered_df)
max_temperature_df = aggregation_max_temperature(filtered_df)
min_temperature_df = aggregation_min_temperature(filtered_df)

# Start streaming and output the results
numRows = 10
query1 = unique_hotels_df.writeStream.outputMode("complete").format("console").option("truncate", False).option("numRows", numRows).start()
query2 = average_temperature_df.writeStream.outputMode("complete").format("console").option("truncate", False).option("numRows", numRows).start()
query3 = max_temperature_df.writeStream.outputMode("complete").format("console").option("truncate", False).option("numRows", numRows).start()
query4 = min_temperature_df.writeStream.outputMode("complete").format("console").option("truncate", False).option("numRows", numRows).start()

# Wait for the streaming to finish
query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()
