import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from spark_session import get_spark

# Chemins d'entrée ONISR. Adaptez si vous placez les CSV ailleurs.
BASE_ONISR = "data/datasets/onisr"
CARACTERISTIQUES_CSV = os.path.join(BASE_ONISR, "caracteristiques_2023.csv")
LIEUX_CSV = os.path.join(BASE_ONISR, "lieux_2023.csv")
VEHICULES_CSV = os.path.join(BASE_ONISR, "vehicules_2023.csv")
USAGERS_CSV = os.path.join(BASE_ONISR, "usagers_2023.csv")

SORTIE_SILVER = "data/output/onsir/silver"
SORTIE_GOLD = "data/output/onsir/gold"


def schema_caracteristiques() -> T.StructType:
    return T.StructType([
        T.StructField("Num_Acc", T.StringType(), nullable=False),
        T.StructField("an", T.IntegerType(), nullable=True),
        T.StructField("jour", T.StringType(), nullable=True),
        T.StructField("heure", T.StringType(), nullable=True),
        T.StructField("dep", T.StringType(), nullable=True),
        T.StructField("agglo", T.StringType(), nullable=True),
        T.StructField("atm", T.StringType(), nullable=True),
        T.StructField("surf", T.StringType(), nullable=True),
        T.StructField("catr", T.StringType(), nullable=True),
        T.StructField("circ", T.StringType(), nullable=True),
        T.StructField("descr_grav", T.StringType(), nullable=True),
    ])


def schema_lieux() -> T.StructType:
    return T.StructType([
        T.StructField("Num_Acc", T.StringType(), nullable=False),
        T.StructField("com", T.StringType(), nullable=True),
        T.StructField("dep", T.StringType(), nullable=True),
        T.StructField("lat", T.DoubleType(), nullable=True),
        T.StructField("long", T.DoubleType(), nullable=True),
        T.StructField("catv", T.StringType(), nullable=True),
    ])


def schema_vehicules() -> T.StructType:
    return T.StructType([
        T.StructField("Num_Acc", T.StringType(), nullable=False),
        T.StructField("num_veh", T.StringType(), nullable=True),
        T.StructField("cat_veh", T.StringType(), nullable=True),
        T.StructField("obs", T.StringType(), nullable=True),
        T.StructField("obsm", T.StringType(), nullable=True),
        T.StructField("choc", T.StringType(), nullable=True),
        T.StructField("manv", T.StringType(), nullable=True),
    ])


def schema_usagers() -> T.StructType:
    return T.StructType([
        T.StructField("Num_Acc", T.StringType(), nullable=False),
        T.StructField("num_veh", T.StringType(), nullable=True),
        T.StructField("catu", T.StringType(), nullable=True),
        T.StructField("sexe", T.StringType(), nullable=True),
        T.StructField("trajet", T.StringType(), nullable=True),
        T.StructField("locp", T.StringType(), nullable=True),
    ])


def check_source_files():
    sources = [
        CARACTERISTIQUES_CSV,
        LIEUX_CSV,
        VEHICULES_CSV,
        USAGERS_CSV,
    ]
    missing = [path for path in sources if not os.path.exists(path)]
    if missing:
        raise FileNotFoundError(
            "Fichiers ONISR introuvables. Vérifiez le dossier data/datasets/onisr/ et les noms de fichiers : "
            + ", ".join(missing)
        )


def lire_csv(spark, chemin, schema):
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "latin1")
        .schema(schema)
        .csv(chemin)
    )


def ingestion(spark):
    check_source_files()

    carac = lire_csv(spark, CARACTERISTIQUES_CSV, schema_caracteristiques())
    lieux = lire_csv(spark, LIEUX_CSV, schema_lieux())
    vehicules = lire_csv(spark, VEHICULES_CSV, schema_vehicules())
    usagers = lire_csv(spark, USAGERS_CSV, schema_usagers())

    print("Caractéristique :", carac.count(), "lignes")
    print("Lieux      :", lieux.count(), "lignes")
    print("Véhicules  :", vehicules.count(), "lignes")
    print("Usagers    :", usagers.count(), "lignes")

    return {
        "caracteristiques": carac,
        "lieux": lieux,
        "vehicules": vehicules,
        "usagers": usagers,
    }


def nettoyage(dfs):
    carac = dfs["caracteristiques"].dropDuplicates()
    lieux = dfs["lieux"].dropDuplicates()
    vehicules = dfs["vehicules"].dropDuplicates()
    usagers = dfs["usagers"].dropDuplicates()

    carac = carac.filter(F.col("Num_Acc").isNotNull())
    carac = carac.filter(F.length(F.trim(F.col("Num_Acc"))) > 0)
    carac = carac.filter(F.col("an").between(2005, 2030))
    carac = carac.withColumn("heure_int", F.when(F.col("heure").rlike("^[0-9]{2}:[0-9]{2}$"), F.substring(F.col("heure"), 1, 2).cast("int")).otherwise(None))
    carac = carac.withColumn("gravite", F.col("descr_grav").cast("int"))
    carac = carac.withColumn("meteo", F.col("atm")).withColumn("etat_route", F.col("surf"))

    lieux = lieux.filter(F.col("Num_Acc").isNotNull())
    lieux = lieux.filter(F.col("dep").isNotNull())
    lieux = lieux.withColumn("dep", F.trim(F.col("dep")))

    vehicules = vehicules.filter(F.col("Num_Acc").isNotNull())
    usagers = usagers.filter(F.col("Num_Acc").isNotNull())

    print("Caractéristique nettoyée :", carac.count(), "lignes")
    print("Lieux nettoyés      :", lieux.count(), "lignes")
    print("Véhicules nettoyés  :", vehicules.count(), "lignes")
    print("Usagers nettoyés    :", usagers.count(), "lignes")

    return {
        "caracteristiques": carac,
        "lieux": lieux,
        "vehicules": vehicules,
        "usagers": usagers,
    }


def ecrire_silver(dfs):
    base = SORTIE_SILVER
    for nom, df in dfs.items():
        chemin = f"{base}/{nom}"
        df.write.mode("overwrite").partitionBy("dep").parquet(chemin)
        print("Silver écrit :", chemin)


def transformation_et_analyses(spark):
    carac = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques")
    lieux = spark.read.parquet(f"{SORTIE_SILVER}/lieux")
    usagers = spark.read.parquet(f"{SORTIE_SILVER}/usagers")

    carac = carac.cache()
    carac.count()

    # Analyse 1 : gravité par météo et état de la route
    agg_meteo = (
        carac
        .groupBy("meteo", "etat_route")
        .agg(
            F.countDistinct("Num_Acc").alias("nb_accidents"),
            F.sum(F.when(F.col("gravite") >= 2, 1).otherwise(0)).alias("nb_graves")
        )
        .orderBy(F.desc("nb_accidents"))
    )

    # Analyse 2 : jointure carac + lieux, accidents par département et heure
    lieux_small = F.broadcast(lieux.select("Num_Acc", "dep"))
    agg_dep_heure = (
        carac
        .join(lieux_small, on="Num_Acc", how="inner")
        .groupBy("dep", "heure_int")
        .agg(F.countDistinct("Num_Acc").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )

    # Analyse 3 : classement des départements par taux d'accidents graves
    dep_stats = (
        carac
        .join(lieux_small, on="Num_Acc", how="inner")
        .groupBy("dep")
        .agg(
            F.countDistinct("Num_Acc").alias("total_accidents"),
            F.sum(F.when(F.col("gravite") >= 2, 1).otherwise(0)).alias("accidents_graves")
        )
        .withColumn("taux_graves", F.col("accidents_graves") / F.col("total_accidents"))
    )

    fenetre = Window.orderBy(F.desc("taux_graves"))
    top_departements = (
        dep_stats
        .withColumn("rang_dep", F.row_number().over(fenetre))
        .filter(F.col("rang_dep") <= 20)
    )

    return {
        "gravite_par_meteo": agg_meteo,
        "accidents_par_dep_heure": agg_dep_heure,
        "top_departements_gravite": top_departements,
    }


def ecrire_gold(resultats):
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print("Résultat gold écrit :", chemin)


def main():
    spark = get_spark("Projet ONISR Jour 4")
    print("Spark UI disponible sur http://localhost:4040")

    dfs = ingestion(spark)
    propre = nettoyage(dfs)
    ecrire_silver(propre)

    resultats = transformation_et_analyses(spark)
    ecrire_gold(resultats)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("Erreur lors de l'exécution du pipeline ONISR :", exc)
        sys.exit(1)
