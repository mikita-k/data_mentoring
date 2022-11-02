from pyspark.sql.functions import lit, udf, when

from hw_spark_basic_homework.src.main.utils import properties, geo

""" NB! Use get_local_data value for switching between local and remote data """
get_local_data = True

geohash_col = "geohash"

""" init UDFs """
get_lat_udf = udf(lambda x, y, z: geo.get_lat(x, y, z))
get_lng_udf = udf(lambda x, y, z: geo.get_lng(x, y, z))
get_geohash_udf = udf(lambda x, y: geo.get_geohash(x, y))


def get_weather_df(spark):
    if get_local_data:
        weather_df = spark.read.option("delimiter", ';').option("header", True).csv("sandbox/weather.csv")
    else:
        path = properties.get_path_weather()
        weather_df = spark.read.parquet(path)

    return weather_df


def get_hotels_df(spark):
    if get_local_data:
        hotels_df = spark.read.option("delimiter", ';').option("header", True).csv("sandbox/hotels.csv")
    else:
        path = properties.get_path_hotels()
        hotels_df = spark.read.options(header='True', inferSchema='True').csv(path)
    return hotels_df


def fix_empty_coordinates(df):
    return df \
        .withColumn("Latitude",
                    when(df.Latitude.isNull(), (
                        get_lat_udf(df.Address, df.City, df.Country)
                    ))
                    .otherwise(df.Latitude)
                    ) \
        .withColumn("Longitude",
                    when(df.Longitude.isNull(), (
                        get_lng_udf(df.Address, df.City, df.Country)
                    ))
                    .otherwise(df.Longitude)
                    )


def add_column_geohash(df):
    return df.withColumn(geohash_col, lit(""))


def fill_column_geohash_w(df):
    return df.withColumn(geohash_col, get_geohash_udf(df.lat, df.lng))


def fill_column_geohash_h(df):
    return df.withColumn(geohash_col, get_geohash_udf(df.Latitude, df.Longitude))
