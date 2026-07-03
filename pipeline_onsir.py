print("Début du script")
import os
import sys
import time
import json
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from spark_session import get_spark

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_ONSIR = "data/dataset/onsir"
CARACTERISTIQUES_CSV = os.path.join(BASE_ONSIR, "caract-2023.csv")
LIEUX_CSV = os.path.join(BASE_ONSIR, "lieux-2023.csv")
VEHICULES_CSV = os.path.join(BASE_ONSIR, "vehicules-2023.csv")
USAGERS_CSV = os.path.join(BASE_ONSIR, "usagers-2023.csv")

SORTIE_SILVER = "data/output/onsir/silver"
SORTIE_GOLD = "data/output/onsir/gold"
SORTIE_LOGS = "data/output/onsir/logs"

# Dictionnaire pour stocker les métriques d'exécution
METRIQUES = {
    "timestamp": datetime.now().isoformat(),
    "etapes": {}
}

# ============================================================================
# VALIDATION ET INGESTION
# ============================================================================

def check_source_files():
    """Vérifie que tous les fichiers source existent."""
    sources = [
        CARACTERISTIQUES_CSV,
        LIEUX_CSV,
        VEHICULES_CSV,
        USAGERS_CSV,
    ]
    missing = [path for path in sources if not os.path.exists(path)]
    if missing:
        raise FileNotFoundError(
            f"Fichiers ONISR introuvables : {', '.join(missing)}\n"
            f"Vérifiez que data/dataset/onsir/ existe avec tous les fichiers."
        )
    print("[✓] Tous les fichiers sources trouvés")


def lire_csv(spark, chemin):
    """Lit un CSV en détectant automatiquement le schéma (inferSchema)."""
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "iso-8859-1")
        .option("inferSchema", "true")
        .csv(chemin)
    )


def ingestion(spark):
    """ÉTAPE 1 : Ingestion des données brutes (Bronze)."""
    print("\n" + "="*70)
    print("ÉTAPE 1 : INGESTION (BRONZE)")
    print("="*70)
    
    check_source_files()
    etape_start = time.time()

    # Lecture des 4 fichiers CSV avec détection automatique du schéma
    carac = lire_csv(spark, CARACTERISTIQUES_CSV)
    lieux = lire_csv(spark, LIEUX_CSV)
    vehicules = lire_csv(spark, VEHICULES_CSV)
    usagers = lire_csv(spark, USAGERS_CSV)

    # Force le comptage pour mesurer le temps réel
    nb_carac = carac.count()
    nb_lieux = lieux.count()
    nb_vehicules = vehicules.count()
    nb_usagers = usagers.count()

    temps_ingestion = time.time() - etape_start

    print(f"\nRésultats ingestion (brut) :")
    print(f"  Caractéristiques : {nb_carac:,} lignes")
    print(f"  Lieux             : {nb_lieux:,} lignes")
    print(f"  Véhicules         : {nb_vehicules:,} lignes")
    print(f"  Usagers           : {nb_usagers:,} lignes")
    print(f"  Temps ingestion   : {temps_ingestion:.2f}s")

    METRIQUES["etapes"]["ingestion"] = {
        "caracteristiques": nb_carac,
        "lieux": nb_lieux,
        "vehicules": nb_vehicules,
        "usagers": nb_usagers,
        "temps_s": round(temps_ingestion, 2)
    }

    return {
        "caracteristiques": carac,
        "lieux": lieux,
        "vehicules": vehicules,
        "usagers": usagers,
    }

# ============================================================================
# NETTOYAGE (BRONZE -> SILVER)
# ============================================================================

def nettoyage(dfs):
    """ÉTAPE 2 : Nettoyage et transformation des données (Silver)."""
    print("\n" + "="*70)
    print("ÉTAPE 2 : NETTOYAGE (SILVER)")
    print("="*70)

    etape_start = time.time()

    # ========== CARACTÉRISTIQUES ==========
    carac = dfs["caracteristiques"].dropDuplicates()
    nb_avant_carac = dfs["caracteristiques"].count()

    carac = carac.filter(F.col("Num_Acc").isNotNull())
    carac = carac.filter(F.length(F.trim(F.col("Num_Acc"))) > 0)
    carac = carac.filter(F.col("an").between(2005, 2030))

    # Heure entière depuis hrmn (format HH:MM)
    carac = carac.withColumn(
        "heure_int",
        F.when(F.col("hrmn").rlike("^[0-9]{2}:[0-9]{2}$"),
               F.substring(F.col("hrmn"), 1, 2).cast("int"))
        .otherwise(None)
    )

    # Renommage métier : atm = condition météo (PAS de gravité dans cette table)
    carac = carac.withColumn("meteo", F.col("atm"))

    nb_apres_carac = carac.count()

    # ========== LIEUX ==========
    lieux = dfs["lieux"].dropDuplicates()
    nb_avant_lieux = dfs["lieux"].count()

    lieux = lieux.filter(F.col("Num_Acc").isNotNull())
    # état de chaussée : vit dans lieux (PAS de "dep" dans cette table)
    lieux = lieux.withColumn("etat_route", F.col("surf"))

    nb_apres_lieux = lieux.count()

    # ========== VÉHICULES ==========
    vehicules = dfs["vehicules"].dropDuplicates()
    nb_avant_vehicules = dfs["vehicules"].count()
    vehicules = vehicules.filter(F.col("Num_Acc").isNotNull())
    nb_apres_vehicules = vehicules.count()

    # ========== USAGERS ==========
    usagers = dfs["usagers"].dropDuplicates()
    nb_avant_usagers = dfs["usagers"].count()
    usagers = usagers.filter(F.col("Num_Acc").isNotNull())
    usagers = usagers.filter(F.col("grav").isNotNull())
    nb_apres_usagers = usagers.count()

    # ========== GRAVITÉ PAR ACCIDENT (dérivée de usagers) ==========
    # grav : 1=indemne, 2=tué, 3=hospitalisé, 4=blessé léger -> ordre PAS croissant en sévérité
    # On construit un score de sévérité réellement croissant avant de prendre un max
    usagers_scored = usagers.withColumn(
        "score_gravite",
        F.when(F.col("grav") == 2, 4)   # tué = le plus grave
         .when(F.col("grav") == 3, 3)   # hospitalisé
         .when(F.col("grav") == 4, 2)   # blessé léger
         .when(F.col("grav") == 1, 1)   # indemne
         .otherwise(None)
    )

    gravite_accident = (
        usagers_scored.groupBy("Num_Acc")
        .agg(
            F.max("score_gravite").alias("gravite_max"),
            F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues"),
            F.sum(F.when(F.col("grav") == 3, 1).otherwise(0)).alias("nb_hospitalises"),
            F.count("*").alias("nb_usagers")
        )
        .withColumn("accident_grave", (F.col("gravite_max") >= 3).cast("int"))
    )

    temps_nettoyage = time.time() - etape_start

    print(f"\nRésultats nettoyage :")
    print(f"  Caractéristiques : {nb_avant_carac:,} → {nb_apres_carac:,} ({nb_avant_carac - nb_apres_carac} écartées)")
    print(f"  Lieux             : {nb_avant_lieux:,} → {nb_apres_lieux:,} ({nb_avant_lieux - nb_apres_lieux} écartées)")
    print(f"  Véhicules         : {nb_avant_vehicules:,} → {nb_apres_vehicules:,} ({nb_avant_vehicules - nb_apres_vehicules} écartées)")
    print(f"  Usagers           : {nb_avant_usagers:,} → {nb_apres_usagers:,} ({nb_avant_usagers - nb_apres_usagers} écartées)")
    print(f"  Accidents avec gravité calculée : {gravite_accident.count():,}")
    print(f"  Temps nettoyage   : {temps_nettoyage:.2f}s")

    METRIQUES["etapes"]["nettoyage"] = {
        "caracteristiques": {"avant": nb_avant_carac, "apres": nb_apres_carac},
        "lieux": {"avant": nb_avant_lieux, "apres": nb_apres_lieux},
        "vehicules": {"avant": nb_avant_vehicules, "apres": nb_apres_vehicules},
        "usagers": {"avant": nb_avant_usagers, "apres": nb_apres_usagers},
        "temps_s": round(temps_nettoyage, 2)
    }

    return {
        "caracteristiques": carac,
        "lieux": lieux,
        "vehicules": vehicules,
        "usagers": usagers,
        "gravite_accident": gravite_accident,
    }

# ============================================================================
# ÉCRITURE SILVER
# ============================================================================

def ecrire_silver(dfs):
    """Sauvegarde la couche Silver en Parquet (sans partitioning pour éviter Hadoop)."""
    print("\n" + "="*70)
    print("SAUVEGARDE COUCHE SILVER (PARQUET)")
    print("="*70)
    
    etape_start = time.time()
    os.makedirs(SORTIE_SILVER, exist_ok=True)
    
    for nom, df in dfs.items():
        chemin = f"{SORTIE_SILVER}/{nom}"
        # ✅ IMPORTANT : coalesce(1) au lieu de partitionBy pour éviter erreurs Hadoop Windows
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print(f"  ✓ {chemin}")

    temps_ecriture = time.time() - etape_start
    print(f"  Temps écriture : {temps_ecriture:.2f}s")
    
    METRIQUES["etapes"]["ecriture_silver"] = {
        "chemin": SORTIE_SILVER,
        "temps_s": round(temps_ecriture, 2)
    }

# ============================================================================
# ANALYSES (SILVER -> GOLD)
# ============================================================================

def transformation_et_analyses(spark):
    """ÉTAPE 3 : Transformations et analyses (Gold)."""
    print("\n" + "="*70)
    print("ÉTAPE 3 : ANALYSES (GOLD)")
    print("="*70)

    carac = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques")
    lieux = spark.read.parquet(f"{SORTIE_SILVER}/lieux")
    gravite = spark.read.parquet(f"{SORTIE_SILVER}/gravite_accident")

    etape_start = time.time()

    carac_cached = carac.cache()
    carac_cached.count()

    # ========== ANALYSE 1 : AGRÉGATION (gravité par météo) ==========
    print("\n[Analyse 1] Gravité par condition météo")
    print("-" * 70)

    analyse1_start = time.time()
    agg_meteo = (
        carac_cached.select("Num_Acc", "meteo")
        .join(gravite, on="Num_Acc", how="inner")
        .groupBy("meteo")
        .agg(
            F.countDistinct("Num_Acc").alias("nb_accidents"),
            F.avg("gravite_max").alias("gravite_moyenne"),
            F.sum("nb_tues").alias("total_tues")
        )
        .filter(F.col("nb_accidents").isNotNull())
        .orderBy(F.desc("nb_accidents"))
    )
    agg_meteo_count = agg_meteo.count()
    analyse1_temps = time.time() - analyse1_start

    print(f"  Résultats : {agg_meteo_count} combinaisons")
    print(f"  Temps d'exécution : {analyse1_temps:.2f}s")
    print("\n  Top 5 :")
    agg_meteo.show(5, truncate=False)

    # ========== ANALYSE 2 : JOINTURE + BROADCAST (accidents par département) ==========
    print("\n[Analyse 2] Accidents par département")
    print("-" * 70)

    carac_dep = carac_cached.select("Num_Acc", "dep")

    # Sans broadcast
    analyse2_sans_start = time.time()
    agg_dep_sans = (
        carac_dep.join(lieux.select("Num_Acc", "etat_route"), on="Num_Acc", how="inner")
        .groupBy("dep")
        .agg(F.countDistinct("Num_Acc").alias("nb_accidents"))
    )
    _ = agg_dep_sans.count()
    temps_sans_broadcast = time.time() - analyse2_sans_start

    # Avec broadcast
    analyse2_broadcast_start = time.time()
    lieux_small = F.broadcast(lieux.select("Num_Acc", "etat_route"))
    agg_dep_heure = (
        carac_dep.join(lieux_small, on="Num_Acc", how="inner")
        .groupBy("dep")
        .agg(F.countDistinct("Num_Acc").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    agg_dep_heure_count = agg_dep_heure.count()
    temps_avec_broadcast = time.time() - analyse2_broadcast_start

    gain = ((temps_sans_broadcast - temps_avec_broadcast) / temps_sans_broadcast * 100) if temps_sans_broadcast > 0 else 0

    print(f"  Résultats : {agg_dep_heure_count} départements")
    print(f"\n  Mesure BROADCAST :")
    print(f"    Sans broadcast  : {temps_sans_broadcast:.2f}s")
    print(f"    Avec broadcast  : {temps_avec_broadcast:.2f}s")
    print(f"    Gain            : {gain:.1f}%")
    print("\n  Top 10 :")
    agg_dep_heure.show(10, truncate=False)

    # ========== ANALYSE 3 : WINDOW FUNCTION (taux de gravité par département) ==========
    print("\n[Analyse 3] Top 20 départements par taux d'accidents graves")
    print("-" * 70)

    analyse3_start = time.time()
    dep_stats = (
        carac_dep
        .join(gravite.select("Num_Acc", "accident_grave"), on="Num_Acc", how="inner")
        .groupBy("dep")
        .agg(
            F.countDistinct("Num_Acc").alias("total_accidents"),
            F.sum("accident_grave").alias("accidents_graves")
        )
        .withColumn(
            "taux_graves",
            F.when(F.col("total_accidents") > 0,
                   F.col("accidents_graves") / F.col("total_accidents"))
            .otherwise(None)
        )
    )

    fenetre = Window.orderBy(F.desc("taux_graves"))
    top_departements = (
        dep_stats
        .withColumn("rang_dep", F.row_number().over(fenetre))
        .filter(F.col("rang_dep") <= 20)
    )
    top_count = top_departements.count()
    analyse3_temps = time.time() - analyse3_start

    print(f"  Résultats : {top_count} départements")
    print(f"  Temps d'exécution : {analyse3_temps:.2f}s")
    print("\n  Top 10 :")
    top_departements.orderBy(F.col("rang_dep")).show(10, truncate=False)

    temps_total_analyses = time.time() - etape_start
    print("\n" + "-" * 70)
    print(f"Temps total analyses : {temps_total_analyses:.2f}s")

    METRIQUES["etapes"]["analyses"] = {
        "analyse_1_meteo": {"lignes": agg_meteo_count, "temps_s": round(analyse1_temps, 2)},
        "analyse_2_dep": {
            "lignes": agg_dep_heure_count,
            "temps_sans_broadcast_s": round(temps_sans_broadcast, 2),
            "temps_avec_broadcast_s": round(temps_avec_broadcast, 2),
            "gain_pourcent": round(gain, 1)
        },
        "analyse_3_window_top_dep": {"lignes": top_count, "temps_s": round(analyse3_temps, 2)},
        "temps_total_s": round(temps_total_analyses, 2)
    }

    return {
        "meteo": agg_meteo,
        "departements": agg_dep_heure,
        "top_departements_graves": top_departements,
    }

# ============================================================================
# ÉCRITURE GOLD
# ============================================================================

def ecrire_gold(resultats):
    """Sauvegarde les résultats de l'analyse (Gold)."""
    print("\n" + "="*70)
    print("SAUVEGARDE COUCHE GOLD (RÉSULTATS)")
    print("="*70)
    
    etape_start = time.time()
    os.makedirs(SORTIE_GOLD, exist_ok=True)
    
    for nom, df in resultats.items():
        try:
            chemin = f"{SORTIE_GOLD}/{nom}"
            df.coalesce(1).write.mode("overwrite").parquet(chemin)
            print(f"  ✓ {chemin}")
        except Exception as e:
            print(f"  ⚠️  Erreur écriture {nom} : {str(e)[:50]}")

    temps_ecriture = time.time() - etape_start
    print(f"  Temps écriture : {temps_ecriture:.2f}s")
    
    METRIQUES["etapes"]["ecriture_gold"] = {
        "chemin": SORTIE_GOLD,
        "temps_s": round(temps_ecriture, 2)
    }

# ============================================================================
# EXPLORATION AU-DELÀ DU COURS
# ============================================================================

def exploration_skew_et_repartitioning(spark):
    """Exploration 1 : Mesure du skew et impact du repartitioning."""
    print("\n" + "="*70)
    print("EXPLORATION : SKEW ET REPARTITIONING")
    print("="*70)

    carac = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques").select("Num_Acc", "dep")
    lieux = spark.read.parquet(f"{SORTIE_SILVER}/lieux").select("Num_Acc", "etat_route")

    print("\n[Mesure 1] Distribution des accidents par département")
    print("-" * 70)

    df_joined = carac.join(F.broadcast(lieux), on="Num_Acc", how="inner")

    skew_avant = df_joined.groupBy("dep").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
    skew_avant.show(10, truncate=False)

    stats_skew = skew_avant.agg(
        F.min("count").alias("min"), F.max("count").alias("max"), F.avg("count").alias("avg")
    ).collect()[0]
    ratio_skew = stats_skew["max"] / stats_skew["avg"] if stats_skew["avg"] > 0 else 0

    print(f"\n  Stats skew : min={stats_skew['min']}, max={stats_skew['max']}, avg={stats_skew['avg']:.0f}")
    print(f"  Ratio max/avg : {ratio_skew:.2f}x")

    print("\n[Mesure 2] Temps d'agrégation : SANS vs AVEC repartitioning")
    print("-" * 70)

    start = time.time()
    _ = df_joined.groupBy("dep").agg(F.count("*")).count()
    temps_sans = time.time() - start

    start = time.time()
    _ = df_joined.repartition(50, "dep").groupBy("dep").agg(F.count("*")).count()
    temps_avec = time.time() - start

    gain = ((temps_sans - temps_avec) / temps_sans * 100) if temps_sans > 0 else 0
    print(f"  Sans repartition : {temps_sans:.2f}s")
    print(f"  Avec repartition : {temps_avec:.2f}s")
    print(f"  Gain : {gain:.1f}%")

    METRIQUES["etapes"]["exploration_skew"] = {
        "skew_ratio": round(ratio_skew, 2),
        "temps_sans_repartition_s": round(temps_sans, 2),
        "temps_avec_repartition_s": round(temps_avec, 2),
        "gain_pourcent": round(gain, 1)
    }


def exploration_cache_impact(spark):
    """Exploration 2 : Mesure de l'impact du cache."""
    print("\n" + "="*70)
    print("EXPLORATION : IMPACT DU CACHE")
    print("="*70)
    
    try:
        carac = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques")
        
        print("\n[Mesure] Agrégation réutilisée : SANS cache vs AVEC cache")
        print("-" * 70)
        
        # SANS cache
        temps_sans = 0
        for i in range(3):
            start = time.time()
            _ = carac.groupBy("meteo").agg(F.count("*")).count()
            t = time.time() - start
            temps_sans += t
            print(f"  Itération {i+1} (sans cache) : {t:.2f}s")
        temps_moyen_sans = temps_sans / 3
        
        # AVEC cache
        carac_cached = carac.cache()
        carac_cached.count()
        
        temps_avec = 0
        for i in range(3):
            start = time.time()
            _ = carac_cached.groupBy("meteo").agg(F.count("*")).count()
            t = time.time() - start
            temps_avec += t
            print(f"  Itération {i+1} (avec cache) : {t:.2f}s")
        temps_moyen_avec = temps_avec / 3
        
        gain = ((temps_moyen_sans - temps_moyen_avec) / temps_moyen_sans * 100) if temps_moyen_sans > 0 else 0
        
        print(f"\n  Temps moyen SANS cache : {temps_moyen_sans:.2f}s")
        print(f"  Temps moyen AVEC cache : {temps_moyen_avec:.2f}s")
        print(f"  Gain : {gain:.1f}%")
        
        carac_cached.unpersist()

        METRIQUES["etapes"]["exploration_cache"] = {
            "temps_moyen_sans_cache_s": round(temps_moyen_sans, 2),
            "temps_moyen_avec_cache_s": round(temps_moyen_avec, 2),
            "gain_pourcent": round(gain, 1)
        }
    except Exception as e:
        print(f"  ⚠️  Exploration cache échouée : {str(e)[:100]}")

# ============================================================================
# SAUVEGARDE DES MÉTRIQUES
# ============================================================================

def sauvegarder_metriques():
    """Sauvegarde les métriques en JSON."""
    os.makedirs(SORTIE_LOGS, exist_ok=True)
    chemin_metriques = os.path.join(SORTIE_LOGS, "metriques.json")
    with open(chemin_metriques, "w", encoding="utf-8") as f:
        json.dump(METRIQUES, f, indent=2, ensure_ascii=False)
    print(f"\n[✓] Métriques sauvegardées : {chemin_metriques}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Fonction principale - orchestration du pipeline complet."""
    spark = get_spark("Projet ONISR Jour 4")
    
    print("\n" + "="*70)
    print("PIPELINE ONISR - ACCIDENTS ROUTIERS 2023")
    print("="*70)
    print(f"Spark UI : http://localhost:4040")
    print(f"Démarrage : {METRIQUES['timestamp']}")

    try:
        # Étapes principales
        dfs = ingestion(spark)
        propre = nettoyage(dfs)
        ecrire_silver(propre)
        
        resultats = transformation_et_analyses(spark)
        ecrire_gold(resultats)
        
        # Explorations au-delà du cours
        exploration_skew_et_repartitioning(spark)
        exploration_cache_impact(spark)
        
        # Sauvegarde des métriques
        sauvegarder_metriques()
        
        print("\n" + "="*70)
        print("✓ PIPELINE COMPLÉTÉ AVEC SUCCÈS")
        print("="*70)
        print(f"\nCouche SILVER  : {SORTIE_SILVER}")
        print(f"Couche GOLD    : {SORTIE_GOLD}")
        print(f"Logs/Métriques : {SORTIE_LOGS}")

    except Exception as exc:
        print(f"\n✗ ERREUR lors de l'exécution : {exc}")
        import traceback
        traceback.print_exc()
        METRIQUES["erreur"] = str(exc)
        sauvegarder_metriques()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()