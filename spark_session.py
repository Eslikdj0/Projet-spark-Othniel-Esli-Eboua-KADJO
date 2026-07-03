import os
from pyspark.sql import SparkSession

# ✅ FIX HADOOP WINDOWS — chemin absolu obligatoire
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = 'C:\\hadoop\\bin;' + os.environ.get('PATH', '')

def get_spark(app_name: str = "Spark Application") -> SparkSession:
    """Crée et renvoie une SparkSession configurée pour le projet."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "200")
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark