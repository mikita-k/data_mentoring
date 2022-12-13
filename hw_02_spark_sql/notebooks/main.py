# Databricks notebook source
# MAGIC %md
# MAGIC ### Initialize properties

# COMMAND ----------

# MAGIC %run ./properties

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", AUTH_TYPE)
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", AUTH_PROVIDER_TYPE)
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", AUTH_CLIENT_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", AUTH_CLIENT_SECRET)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", AUTH_CLIENT_ENDPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load source data as DataFrames

# COMMAND ----------

expedia_df = spark.read.format("avro").load(PATH_EXPEDIA)

print(f"expedia count is:{expedia_df.count()}")
# display(expedia_df)

# COMMAND ----------

hotel_weather_df = spark.read.parquet(PATH_HOTEL_WEATHER)

print(f"hotel-weather count is: {hotel_weather_df.count()}")
# display(hotel_weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create delta tables based on data in storage account.

# COMMAND ----------

r_expedia = "r_expedia"
expedia_df.write.mode("overwrite").saveAsTable(r_expedia)

# COMMAND ----------

r_hotel_weather = "r_hotel_weather"
hotel_weather_df.write.mode("overwrite").saveAsTable(r_hotel_weather)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Spark SQL calculate and visualize in Databricks Notebooks (for queries use hotel_id - join key, srch_ci- checkin, srch_co - checkout:
# MAGIC * #### Top 10 hotels with max absolute temperature difference by month.

# COMMAND ----------

# calculate top 10 hotels with max abs temperature separated by month
from pyspark.sql.functions import *

# load data from db
hotel_weather_df = spark.sql("SELECT * FROM r_hotel_weather") \
    .select("address", "name", "avg_tmpr_c", "wthr_date", "year", "month") \
    .orderBy("name", "address", "wthr_date")

# calculate average temperature for all hotels group by month
average_tmpr_df = hotel_weather_df.groupBy(hotel_weather_df.name, hotel_weather_df.address, "year", "month") \
    .agg(avg("avg_tmpr_c").alias("visit_avg_tmpr")) \
    .select(col("name").alias("address"), col("address").alias("name"), "year", "month", "visit_avg_tmpr") \
    .orderBy("name", "address")
display(average_tmpr_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * #### Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.

# COMMAND ----------

# calculate TOP 10 EVER
top_10_busy_hotels_query = f"""
WITH r_visits AS
(SELECT re.id, rhw.name, rhw.address, srch_adults_cnt + srch_children_cnt as visits_cnt,
DATE_TRUNC('month', srch_ci) as check_in,
DATE_TRUNC('month', srch_co) as check_out
FROM {r_expedia} re
LEFT JOIN (SELECT id, name, address FROM {r_hotel_weather} GROUP BY id, name, address) rhw ON (rhw.id = re.hotel_id)

WHERE name IS NOT NULL

), r_visit_date as
(SELECT check_in as visit_date FROM r_visits
GROUP BY check_in
UNION
SELECT check_out as visit_date FROM r_visits
GROUP BY check_out
)

SELECT name, address, visit_date, sum(visits_cnt) as guest_cnt
FROM r_visit_date rvd
LEFT JOIN r_visits rv ON (rv.check_in <= rvd.visit_date AND rv.check_out >= rvd.visit_date)
GROUP BY name, address, visit_date
ORDER BY guest_cnt DESC

LIMIT 10
"""

top_10_busy_hotels_df = spark.sql(top_10_busy_hotels_query)
display(top_10_busy_hotels_df)


# COMMAND ----------

# calculate (TOP 10 hotels every month)
# TODO - add visualization
top_cnt = 10

top_10_busy_hotels_ever_query = f"""
WITH r_visits AS
(SELECT rhw.name, rhw.address, srch_adults_cnt + srch_children_cnt AS visits_cnt,
DATE_TRUNC('month', srch_ci) AS checkin,
DATE_TRUNC('month', srch_co) AS checkout
FROM {r_expedia} re
LEFT JOIN (SELECT id, name, address FROM {r_hotel_weather} GROUP BY id, name, address) rhw ON (rhw.id = re.hotel_id)

WHERE name IS NOT NULL

), r_visit_date AS
(SELECT checkin as visit_date FROM r_visits
GROUP BY visit_date
UNION
SELECT checkout as visit_date FROM r_visits
GROUP BY visit_date

), r_grouped_visits AS
(SELECT ROW_NUMBER() OVER(ORDER BY visit_date, sum(visits_cnt) DESC) AS row_nr, address, visit_date, sum(visits_cnt) as total_visits FROM r_visit_date rvd
LEFT JOIN r_visits rv ON (rv.checkin <= rvd.visit_date AND rv.checkout >= rvd.visit_date)
GROUP BY address, visit_date
ORDER BY address, visit_date)

SELECT rgv.address, rgv.visit_date, rgv.total_visits
FROM r_grouped_visits rgv
LEFT JOIN (SELECT visit_date, MIN(row_nr) as min_row_nr FROM r_grouped_visits GROUP BY visit_date) rgv_filter
ON (rgv.visit_date = rgv_filter.visit_date AND rgv.row_nr < (rgv_filter.min_row_nr + {top_cnt}))

WHERE rgv_filter.min_row_nr IS NOT NULL
AND rgv.address IS NOT NULL

ORDER BY row_nr
"""

top_10_busy_hotels_ever_df = spark.sql(top_10_busy_hotels_ever_query)
display(top_10_busy_hotels_ever_df)


# COMMAND ----------

# MAGIC %md
# MAGIC * #### For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

# COMMAND ----------

from pyspark.sql.functions import *

# load data from db
expedia_df = spark.sql("SELECT * FROM r_expedia")
hotel_weather_df = spark.sql("SELECT * FROM r_hotel_weather") \
    .select("id", "address", "name", "avg_tmpr_c", "wthr_date")


# filter all hotels
hotels_df = hotel_weather_df.groupBy("id", "address", "name").agg(max("id").alias("hotel_id"))
# display(hotels_df)


# filter long term visits
expedia_long_term_df = expedia_df.filter(datediff(col("srch_co"), col("srch_ci")) >= 7) \
    .select("id", "hotel_id", "srch_ci", "srch_co")
# display(expedia_long_term_df)


# join visits and weathers to find average values
join_condition_1 = [
    expedia_long_term_df.hotel_id == hotel_weather_df.id,
    expedia_long_term_df.srch_ci <= hotel_weather_df.wthr_date,
    expedia_long_term_df.srch_co >= hotel_weather_df.wthr_date,
]
join_df = expedia_long_term_df \
    .join(hotel_weather_df, join_condition_1, "left") \
    .select(
        expedia_long_term_df.id,
        hotel_weather_df.name,
        hotel_weather_df.address,
        expedia_long_term_df.srch_ci,
        expedia_long_term_df.srch_co,
        hotel_weather_df.avg_tmpr_c,
        hotel_weather_df.wthr_date   
    ) 
# display(join_df)


# calculate average temperature for all visits
average_tmpr_df = join_df.groupBy(join_df.id) \
    .agg(avg("avg_tmpr_c").alias("visit_avg_tmpr")) \
    .select(col("id").alias("visit_id"), "visit_avg_tmpr")
# display(average_tmpr_df)


# join visits, average temperatures and hotels
join_condition_2 = [
    expedia_long_term_df.id == average_tmpr_df.visit_id
]
join_condition_3 = [
    expedia_long_term_df.hotel_id == hotels_df.hotel_id
]
result_df = expedia_long_term_df \
    .join(average_tmpr_df, join_condition_2, "left") \
    .join(hotels_df, join_condition_3, "left") \
    .select(
        expedia_long_term_df.id,
        col("address"),
        col("name"),
        expedia_long_term_df.srch_ci,
        expedia_long_term_df.srch_co,
        average_tmpr_df.visit_avg_tmpr
    ) \
    .where(col("address").isNotNull()) \
    .orderBy("address", "name", "srch_ci")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Store final DataMarts and intermediate data (joined data with all the fields from both datasets) in provisioned with terraform Azure ADLS gen2 storage preserving data partitioning in parquet format in “data” container (it marked with prevent_destroy=true and will survive terraform destroy).
# MAGIC 
# MAGIC 
# MAGIC #### Expected results:
# MAGIC 
# MAGIC * ##### Repository with notebook (with output results), configuration scripts, application sources, execution plan dumps, analysis and etc.
# MAGIC * ##### Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clear db

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS r_hotel_weather")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS r_expedia")
