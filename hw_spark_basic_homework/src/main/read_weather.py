import hw_spark_basic_homework.src.main.utils.dataframe as df_utils


def get_weather(spark):
    weather_df = df_utils.get_weather_df(spark)
    weather_df = df_utils.add_column_geohash(weather_df)
    weather_df = df_utils.fill_column_geohash_w(weather_df)

    return weather_df
