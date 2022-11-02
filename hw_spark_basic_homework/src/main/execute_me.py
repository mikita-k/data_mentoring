from hw_spark_basic_homework.src.main.read_hotels import get_hotels
from hw_spark_basic_homework.src.main.read_weather import get_weather
from hw_spark_basic_homework.src.main.utils.spark import get_spark

""" init spark """
spark = get_spark()

""" load weather """
weather_df = get_weather(spark)
print(f"weather cnt {weather_df.count()}")
weather_df.show()

""" load hotels """
hotels_df = get_hotels(spark)
print(f"hotels cnt {hotels_df.count()}")
hotels_df.show()

""" join weather and hotels """
join_df = hotels_df.join(weather_df, hotels_df.geohash == weather_df.geohash, "left")
print(f"join cnt {join_df.count()}")
join_df.show(25)
