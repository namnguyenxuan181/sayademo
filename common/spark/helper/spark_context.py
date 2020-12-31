import warnings
from typing import Dict

import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

from common.minio import connection

__all__ = ['init_spark']

warnings.warn(
    "This module is deprecated. Please use `SparkSession.builder`.",
    DeprecationWarning, stacklevel=2
)


def init_spark(
    app_name: str = None,
    master: str = None,
    endpoint: str = None,
    username: str = None,
    password: str = None,
    parallelism: int = None,
    partitions: int = None,
    configs: Dict[str, str] = None,
) -> SparkSession:
    """
    Initialize a spark session, auto detect spark master via findspark

    Args:
        app_name: spark.app.name - Application name
        master: spark.master. If None, findspark will be used to automatically detect spark master
        endpoint: spark.hadoop.fs.s3a.endpoint. If None, MINIO_ENDPOINT from env will be used
        username: spark.hadoop.fs.s3a.access.key. If None, MINIO_USERNAME from env will be used
        password: spark.hadoop.fs.s3a.secret.key. If None, MINIO_PASSWORD from env will be used
        parallelism: spark.default.parallelism
        partitions: spark.sql.shuffle.partitions
        configs: additional configurations. Priority: kwargs -> configs -> DEFAULT_CONFIGS
    Return: SparkSession
    """
    warnings.warn(
        """This function is deprecated. Please use `SparkSession.builder`.""",
        DeprecationWarning, stacklevel=3
    )

    kwargs_configs = {k: v for k, v in [
        ('spark.app.name', app_name),
        ('spark.master', master),
        ('spark.hadoop.fs.s3a.endpoint', endpoint),
        ('spark.hadoop.fs.s3a.access.key', username),
        ('spark.hadoop.fs.s3a.secret.key', password),
        ('spark.default.parallelism', parallelism),
        ('spark.sql.shuffle.partitions', partitions),
    ] if v is not None}

    access_key = connection.get_access_key()
    secret_key = connection.get_secret_key()
    endpoint = connection.get_endpoint(protocol=True)

    default_configs = {
        'spark.hadoop.fs.s3a.endpoint': endpoint,
        'spark.hadoop.fs.s3a.access.key': access_key,
        'spark.hadoop.fs.s3a.secret.key': secret_key,
        'spark.default.parallelism': '20',
        'spark.sql.shuffle.partitions': '20',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.ui.showConsoleProgress': 'false',
    }

    final_configs = {
        **default_configs,
        **(configs or {}),
        **kwargs_configs,
    }

    conf = SparkConf().setAll(final_configs.items())

    if 'spark.master' not in final_configs:
        # Auto detect spark master if not provided
        findspark.init()

    return SparkSession.builder.config(conf=conf).getOrCreate()
