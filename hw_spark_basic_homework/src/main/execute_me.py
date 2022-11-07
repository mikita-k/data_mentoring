from hw_spark_basic_homework.src.main.read_hotels import get_hotels
from hw_spark_basic_homework.src.main.read_weather import get_weather
from hw_spark_basic_homework.src.main.utils.spark import get_spark

""" switch between local and remote data """
get_local_data = False

""" init spark """
spark = get_spark(get_local_data)

""" load weather """
weather_df = get_weather(spark)

""" load hotels """
hotels_df = get_hotels(spark)

""" join weather and hotels """
join_df = hotels_df.join(weather_df, hotels_df.geohash == weather_df.ghsh, "left")

""" save data to zipped csv """
join_df.write.option("header", True) \
    .option("compression", "gzip") \
    .mode("overwrite") \
    .csv("output/result")
