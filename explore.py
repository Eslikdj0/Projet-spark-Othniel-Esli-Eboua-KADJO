from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Onsir_projet")
    .master("local[*]")            # mode local, tous les coeurs
    .getOrCreate()
)

# Reduire le bruit dans la console
spark.sparkContext.setLogLevel("WARN")

print("Version de Spark :", spark.version)
print("Master :", spark.sparkContext.master)

chemin = "data/datasets/yellow_tripdata_2024-01.parquet"

df = spark.read.parquet(chemin)

df.show(5, truncate=False)
df.printSchema()

nb = df.count()

print("Nombre de courses :", nb)

input("Spark UI : http://localhost:4040 (Appuyez sur Entrée pour quitter)");
spark.stop()
