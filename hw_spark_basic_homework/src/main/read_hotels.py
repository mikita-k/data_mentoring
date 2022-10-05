from pyspark.sql.functions import udf, when, lit

from hw_spark_basic_homework.src.main.utils import properties, geo
from hw_spark_basic_homework.src.main.utils.spark import get_spark


def get_hotels():
    """ init UDFs """
    get_lat_udf = udf(lambda x, y, z: geo.get_lat(x, y, z))
    get_lng_udf = udf(lambda x, y, z: geo.get_lng(x, y, z))
    get_geohash_udf = udf(lambda x, y: geo.get_geohash(x, y))

    """ Init Spark """
    spark = get_spark()
    path = properties.get_path_hotels()

    """ read hotels """
    hotels_df = spark.read.options(header='True', inferSchema='True').csv(path)

    """ generate hotels dataframe based on input data, replace null values using geocode API """
    hotels_null_free_df = hotels_df \
        .withColumn("Latitude",
                    when(hotels_df.Latitude.isNull(), (
                        get_lat_udf(hotels_df.Address, hotels_df.City, hotels_df.Country)
                    ))
                    .otherwise(hotels_df.Latitude)
                    ) \
        .withColumn("Longitude",
                    when(hotels_df.Longitude.isNull(), (
                        get_lng_udf(hotels_df.Address, hotels_df.City, hotels_df.Country)
                    ))
                    .otherwise(hotels_df.Longitude)
                    ) \
        .withColumn("Geohash", lit(""))

    """ init geohash """
    hotels_with_geohash_df = hotels_null_free_df \
        .withColumn("Geohash", get_geohash_udf(hotels_null_free_df.Latitude, hotels_null_free_df.Longitude))

    return hotels_with_geohash_df
