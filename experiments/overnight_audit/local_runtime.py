"""Explicit local execution for read-only experiments; never submits a Pod."""


def spark_session(name, local_workers):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(name)
    if local_workers:
        if not 1 <= local_workers <= 32:
            raise ValueError('Local experiments require 1..32 workers')
        builder = (builder.master(f'local[{local_workers}]')
                   .config('spark.driver.bindAddress', '127.0.0.1')
                   .config('spark.driver.host', '127.0.0.1'))
    spark = builder.getOrCreate()
    master = spark.sparkContext.master
    assert (master == f'local[{local_workers}]') if local_workers else master.startswith('k8s://')
    return spark
