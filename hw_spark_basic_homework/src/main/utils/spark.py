import findspark
from pyspark.sql import SparkSession

from hw_spark_basic_homework.src.main.utils import properties


def get_spark(get_local_data):
    if get_local_data:
        return get_spark_local()
    else:
        return get_spark_remote()


def get_spark_local():
    findspark.init()
    spark = SparkSession \
        .builder \
        .config("get_local_data", True) \
        .appName('sandbox') \
        .getOrCreate()
    return spark


def get_spark_remote():
    spark = SparkSession.builder \
        .appName("test spark session") \
        .config("get_local_data", False) \
        .config("spark.some.config.option", "some-value") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1,com.azure:azure-storage-blob:12.18.0') \
        .config("spark.executor.heartbeatInterval", "600s") \
        .config("spark.network.timeout", "900s") \
        .getOrCreate()

    spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", properties.get_auth_type())
    spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
                   properties.get_auth_provider_type())
    spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", properties.get_auth_client_id())
    spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
                   properties.get_auth_client_secret())
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
                   properties.get_auth_client_endpoint())

    sc = spark.sparkContext

    return spark
