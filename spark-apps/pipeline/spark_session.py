import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "frailty-pipeline", driver_memory: str | None = None) -> SparkSession:
    """
    Crea y devuelve una SparkSession configurada para:
    - S3A (MinIO en local, S3 nativo en AWS): credenciales y endpoint desde env.
    - Delta Lake 4.0: extensión SQL y catálogo registrados.

    Los JARs necesarios (hadoop-aws, delta-spark, kafka-connector, etc.) ya
    están en /opt/spark/jars/ de la imagen Docker, por lo que no es necesario
    pasar spark.jars.packages.
    """
    # Airflow containers expose AWS_ACCESS_KEY_ID; spark-master only has SPARK_CONF_* vars.
    endpoint   = (os.getenv("MINIO_ENDPOINT")
                  or os.getenv("SPARK_CONF_spark.hadoop.fs.s3a.endpoint",
                                "http://minio:9000"))
    access_key = (os.getenv("AWS_ACCESS_KEY_ID")
                  or os.getenv("SPARK_CONF_spark.hadoop.fs.s3a.access.key", ""))
    secret_key = (os.getenv("AWS_SECRET_ACCESS_KEY")
                  or os.getenv("SPARK_CONF_spark.hadoop.fs.s3a.secret.key", ""))

    mem = driver_memory or os.getenv("SPARK_DRIVER_MEMORY", "2g")

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", mem)

        # --- Delta Lake ---
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # --- S3A / MinIO ---
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",            endpoint)
        .config("spark.hadoop.fs.s3a.access.key",          access_key)
        .config("spark.hadoop.fs.s3a.secret.key",          secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Evita intentar resolver credenciales desde EC2/IAM en entorno local.
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        .getOrCreate()
    )
