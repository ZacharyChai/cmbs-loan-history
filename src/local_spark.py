"""A local Spark session with Delta and the XML reader, for tests and local runs.

ANSI mode is on because Databricks serverless runs with it on: a cast or date parse that
would raise there raises here too, instead of quietly returning null.

The XML reader is the open-source spark-xml package. Databricks has its own built-in
reader with the same rowTag option; the notebooks call the same format("xml") code.
"""

import os
import tempfile

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(warehouse_dir: str | None = None) -> SparkSession:
    warehouse = warehouse_dir or tempfile.mkdtemp(prefix="cmbs_wh_")
    os.makedirs(warehouse, exist_ok=True)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("cmbs-loan-history-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "3g")
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["com.databricks:spark-xml_2.12:0.18.0"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
