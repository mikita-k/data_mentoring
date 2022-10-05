from hw_spark_basic_homework.src.main.read_hotels import get_hotels
from hw_spark_basic_homework.src.main.read_weather import get_weather

weather_df = get_weather()
# print(f"weather cnt {weather_df.count()}")
hotels_df = get_hotels()
# print(f"hotel cnt {hotels_df.count()}")

# TODO nex part failed due to SocketTimeoutException
join_df = weather_df.join(hotels_df, weather_df.Geohash == hotels_df.Geohash, "left")
join_df.show(truncate=False)
print(join_df.count())
