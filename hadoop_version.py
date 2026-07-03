from spark_session import get_spark

spark = get_spark("Test ecriture parquet")
df = spark.createDataFrame([(1, "test")], ["id", "valeur"])
df.write.mode("overwrite").parquet("data/output/test_winutils")
print("✓ Écriture Parquet réussie")
spark.stop()
