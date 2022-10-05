from pyspark.sql.functions import udf, lit

from hw_spark_basic_homework.src.main.utils import properties, geo
from hw_spark_basic_homework.src.main.utils.spark import get_spark


def get_weather():
    """ init UDFs """
    get_geohash_udf = udf(lambda x, y: geo.get_geohash(x, y))

    """ Init Spark """
    spark = get_spark()
    path = properties.get_path_weather()

    """ read weather """
    weather_df = spark.read.parquet(path)

    """ add new column Geohash """
    weather_df_2 = weather_df \
        .withColumn("Geohash", lit(""))

    """ init geohash """
    weather_with_geohash_df = weather_df_2 \
        .withColumn("Geohash", get_geohash_udf(weather_df_2.lat, weather_df_2.lng))

    return weather_with_geohash_df
