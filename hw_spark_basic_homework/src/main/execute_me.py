from hw_spark_basic_homework.src.main.read_hotels import get_hotels
from hw_spark_basic_homework.src.main.read_weather import get_weather
from hw_spark_basic_homework.src.main.utils.spark import get_spark

""" init spark """
spark = get_spark()

weather_df = get_weather(spark)
print(f"weather cnt {weather_df.count()}")

hotels_df = get_hotels(spark)
print(f"hotel cnt {hotels_df.count()}")

# TODO nex part failed due to SocketTimeoutException
join_df = weather_df.join(hotels_df, weather_df.Geohash == hotels_df.Geohash, "left")
join_df.show(truncate=False)
print(join_df.count())
