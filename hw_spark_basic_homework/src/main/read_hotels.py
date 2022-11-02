import hw_spark_basic_homework.src.main.utils.dataframe as df_utils


def get_hotels(spark):
    hotels_df = df_utils.get_hotels_df(spark)
    hotels_df = df_utils.fix_empty_coordinates(hotels_df)
    hotels_df = df_utils.add_column_geohash(hotels_df)
    hotels_df = df_utils.fill_column_geohash_h(hotels_df)

    return hotels_df
