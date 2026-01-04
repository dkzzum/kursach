from pyspark.sql import SparkSession

access_token = '15f56a5915f56a5915f56a595116cb70c8115f515f56a597cb79e1252b183d4ccc4f7fd'
api_id = 31634371
api_hash = '603a033c91d43d322826945ed9b9e750'
phone = 'Samsung Galaxy S23'
app_version = 'Telegram Android 12.2.3'
system_lang_code = 'en'
lang_code = 'en'
session = 'session1'


def get_spark_session(app_name="TelegramToxicAnalysis"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/data/metastore_db;create=true") \
        .enableHiveSupport() \
        .getOrCreate()
