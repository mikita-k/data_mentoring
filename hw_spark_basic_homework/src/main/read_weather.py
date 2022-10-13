from pyspark.sql.functions import udf, lit

from hw_spark_basic_homework.src.main.utils import properties, geo


def get_weather(spark):
    """ init UDFs """
    get_geohash_udf = udf(lambda x, y: geo.get_geohash(x, y))

    """ read weather """
    path = properties.get_path_weather()
    weather_df = spark.read.parquet(path)

    """ add new column Geohash """
    weather_df_2 = weather_df \
        .withColumn("Geohash", lit(""))

    """ init geohash """
    weather_with_geohash_df = weather_df_2 \
        .withColumn("Geohash", get_geohash_udf(weather_df_2.lat, weather_df_2.lng))

    return weather_with_geohash_df
