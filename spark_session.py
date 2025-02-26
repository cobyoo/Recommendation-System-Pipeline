from pyspark.sql import SparkSession

def create_spark_session():
    hadoop_aws_jar = "/home/elicer/spark/jars/hadoop-aws-3.3.4.jar"
    aws_sdk_jar = "/home/elicer/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    iceberg_spark_jar = "/home/elicer/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
    hadoop_common_jar = "/home/elicer/spark/jars/hadoop-common-3.3.4.jar"

    s3_access_key = "svY88fVSdjCfCUbNWgXcxoUCcA0"
    s3_secret_key = "hUNykBsfJQhOdvw6vdsETwSyoPKC+HNITZeO9Dbi9hA"
    iceberg_metadata_location = "s3a://ai-helpy-8qgojtvd/recommendations"

    spark = SparkSession.builder \
        .appName("IcebergIntegration") \
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{iceberg_spark_jar},{hadoop_common_jar}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", iceberg_metadata_location) \
        .config("fs.s3a.access.key", s3_access_key) \
        .config("fs.s3a.secret.key", s3_secret_key) \
        .config("fs.s3a.endpoint", "https://datahub-central-01.elice.io") \
        .config("fs.s3a.connection.ssl.enabled", "true") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.cores", "2") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.ui.retainedStages", "10") \
        .config("spark.ui.retainedJobs", "10") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    return spark
