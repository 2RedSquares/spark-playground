from pyspark.sql.types import StructField, StructType, StringType

# rename columns with invalid names
# this causes problems when saving to Delta
# Original Schema
"""
spark_conf:struct
  fs.s3n.awsAccessKeyId:string
  fs.s3n.awsSecretAccessKey:string
  spark.conf.set("spark.driver.extraClassPath",:string
  spark.conf.set("spark.executor.extraClassPath",:string
  spark.databricks.acl.enabled:string
"""


sparkConfSchema = StructType([
  StructField("fs_s3n_awsAccessKeyId", StringType(), True),
  StructField("fs_s3n_awsAccessKeyId", StringType(), True),
  StructField("spark_driver_extraClassPath", StringType(), True),
  StructField("spark_executor_extraClassPath", StringType(), True),
  StructField("spark_databricks_acl_enabled", StringType(), True),
  StructField("spark_databricks_acl_sqlOnly", StringType(), True),
  StructField("spark_databricks_cluster_profile", StringType(), True),
  StructField("spark_databricks_delta_preview_enabled", StringType(), True),
  StructField("spark_databricks_repl_allowedLanguages", StringType(), True),
  StructField("spark_driver_maxResultSize", StringType(), True),
  StructField("spark_hadoop_fs_s3_impl", StringType(), True),
  StructField("spark_hadoop_fs_s3a_acl_default", StringType(), True),
  StructField("spark_hadoop_fs_s3a_canned_acl", StringType(), True),
  StructField("spark_hadoop_fs_s3a_impl", StringType(), True),
  StructField("spark_hadoop_fs_s3n_impl", StringType(), True),
  StructField("spark_hadoop_hive_metastore_uris", StringType(), True),
  StructField("spark_speculation", StringType(), True),
  StructField("spark_sql_autoBroadcastJoinThreshold", StringType(), True),
  StructField("spark_sql_hive_metastore_jars", StringType(), True),
  StructField("spark_sql_hive_metastore_schema_verification", StringType(), True),
  StructField("spark_sql_hive_metastore_schema_verification_record_version", StringType(), True),
  StructField("spark_sql_hive_metastore_version", StringType(), True),
  StructField("spark_sql_streaming_metricsEnabled", StringType(), True)
])

df = clusterDF.select(col("spark_conf").cast(sparkConfSchema))
