# Spark SQL : vues temporaires et équivalence avec l'API DataFrame

## Objectif

Apprendre à exposer un DataFrame comme une table SQL avec `createOrReplaceTempView`, écrire
des requêtes en `spark.sql(...)`, et constater que les mêmes traitements écrits en API
DataFrame et en Spark SQL produisent le même résultat et le même plan d'exécution. À la fin
de ce TP, vous savez choisir l'une ou l'autre approche en connaissance de cause, et vous savez
le prouver avec `explain()`.

## Contexte

On reste sur le fil rouge des courses de taxi jaunes de New York (NYC TLC). Le fichier
`yellow_tripdata_2024-01.parquet` contient environ 3 millions de courses pour janvier 2024, et
la table des zones `taxi_zone_lookup.csv` donne le nom de chaque zone à partir de son
identifiant (`LocationID`). On suppose les deux fichiers présents dans `data/datasets/`.

Au jour 2, vous avez déjà manipulé ces données avec l'API DataFrame (`select`, `filter`,
`groupBy`, `agg`, `join`). Spark SQL n'est pas un autre moteur : c'est une autre façade sur le
même moteur. Les deux approches passent par l'optimiseur Catalyst et finissent en un plan
physique identique. Le but de ce TP est de le vérifier vous-même, requête par requête.

Colonnes utiles : `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`,
`trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`, `total_amount`,
`payment_type`.

## Consignes

### 1. Charger les données et créer les vues temporaires

Créez un fichier `tp04_spark_sql.py`. Chargez le Parquet des courses et le CSV des zones, puis
exposez chacun comme une vue temporaire avec `createOrReplaceTempView`. Une vue temporaire est
un nom de table valable uniquement dans la session Spark courante :

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP04 - Spark SQL")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
)

# A completer : exposer les deux DataFrames comme des vues temporaires
courses.createOrReplaceTempView("________")
zones.createOrReplaceTempView("________")

# Verifier que les vues existent
spark.sql("SHOW TABLES").show()
```

### 2. Filtrer : écrire la même requête en API et en SQL

On veut les courses payées par carte (`payment_type = 1`) dont le montant total est strictement
positif, en ne gardant que quelques colonnes. Écrivez les deux versions et vérifiez qu'elles
renvoient le même nombre de lignes :

```python
# Version API DataFrame
api_filtre = (
    courses
    .filter((F.col("payment_type") == 1) & (F.col("total_amount") > 0))
    .select("tpep_pickup_datetime", "trip_distance", "total_amount")
)

# Version Spark SQL : a completer
sql_filtre = spark.sql("""
    SELECT tpep_pickup_datetime, trip_distance, total_amount
    FROM ________
    WHERE ________ = 1 AND ________ > 0
""")

print("API :", api_filtre.count())
print("SQL :", sql_filtre.count())
```

### 3. Agréger : revenu et nombre de courses par zone de départ

Calculez, par zone de départ (`PULocationID`), le nombre de courses et le revenu total
(`total_amount`), trié par revenu décroissant. Écrivez la version API, puis la version SQL :

```python
# Version API DataFrame
api_agg = (
    courses
    .groupBy("PULocationID")
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu_total"),
    )
    .orderBy(F.col("revenu_total").desc())
)

# Version Spark SQL : a completer
sql_agg = spark.sql("""
    SELECT
        PULocationID,
        COUNT(*) AS nb_courses,
        ROUND(SUM(total_amount), 2) AS revenu_total
    FROM courses
    GROUP BY ________
    ORDER BY ________ DESC
""")

api_agg.show(5)
sql_agg.show(5)
```

### 4. Joindre : remplacer l'identifiant de zone par son nom

L'identifiant `PULocationID` n'est pas parlant. Joignez l'agrégat précédent à la table des
zones pour afficher le nom du quartier (`Borough`) et de la zone (`Zone`). Faites la jointure
une fois en API DataFrame, une fois en SQL :

```python
# Version API DataFrame
api_join = (
    api_agg
    .join(zones, api_agg.PULocationID == zones.LocationID, "left")
    .select("Borough", "Zone", "nb_courses", "revenu_total")
    .orderBy(F.col("revenu_total").desc())
)

# Version Spark SQL : a completer
sql_join = spark.sql("""
    SELECT z.Borough, z.Zone, a.nb_courses, a.revenu_total
    FROM (
        SELECT PULocationID, COUNT(*) AS nb_courses,
               ROUND(SUM(total_amount), 2) AS revenu_total
        FROM courses
        GROUP BY PULocationID
    ) AS a
    LEFT JOIN ________ AS z ON a.PULocationID = z.________
    ORDER BY a.revenu_total DESC
""")

api_join.show(10, truncate=False)
sql_join.show(10, truncate=False)
```

### 5. Comparer les deux plans d'exécution

C'est le cœur du TP. Affichez le plan physique des deux versions de l'agrégation (consigne 3)
et comparez-les. Ils doivent être identiques ou quasi identiques :

```python
print("===== Plan API =====")
api_agg.explain()        # ou explain(mode="formatted")

print("===== Plan SQL =====")
sql_agg.explain()
```

Repérez dans les deux plans : le scan Parquet (`FileScan parquet`), l'agrégation
(`HashAggregate`) et le shuffle (`Exchange`). Notez ce que vous observez : les deux plans
sont-ils les mêmes ?

### 6. Bonus : une requête plus naturelle en SQL

Certaines questions s'écrivent plus vite en SQL. Répondez à la question suivante uniquement en
SQL : pour chaque tranche horaire de prise en charge (heure de `tpep_pickup_datetime`), quel est
le nombre de courses et le pourboire moyen, pour les seuls paiements par carte ?

```python
sql_bonus = spark.sql("""
    SELECT
        HOUR(tpep_pickup_datetime) AS heure,
        COUNT(*) AS nb_courses,
        ROUND(AVG(tip_amount), 2) AS pourboire_moyen
    FROM courses
    WHERE payment_type = 1
    GROUP BY HOUR(tpep_pickup_datetime)
    ORDER BY heure
""")
sql_bonus.show(24)
```

## Livrable

Vous avez réussi le TP si :

- Les deux vues temporaires sont créées et apparaissent dans `spark.sql("SHOW TABLES")`.
- Pour le filtre (consigne 2), la version API et la version SQL renvoient **exactement le même
  nombre de lignes**.
- Pour l'agrégation (consigne 3), les cinq premières lignes des deux versions sont identiques
  (mêmes zones, mêmes revenus, même ordre).
- La jointure (consigne 4) affiche bien des noms de zones (par exemple des quartiers de
  Manhattan en tête) au lieu des seuls identifiants.
- Vous avez comparé les deux plans d'exécution (consigne 5) et vous pouvez expliquer en une
  phrase pourquoi ils sont identiques.
- Le bonus (consigne 6) sort une ligne par heure (0 à 23) avec le pourboire moyen.

## Aide

### Rappels d'API

- Créer une vue : `df.createOrReplaceTempView("nom")`. Le `OrReplace` évite l'erreur si une vue
  du même nom existe déjà. La vue vit le temps de la SparkSession.
- Lancer une requête : `spark.sql("SELECT ...")` renvoie un DataFrame normal. On peut donc lui
  rechaîner de l'API : `spark.sql("...").filter(...).show()`.
- Lister les vues : `spark.sql("SHOW TABLES").show()` ou `spark.catalog.listTables()`.
- Voir le plan : `df.explain()` (plan physique) ou `df.explain(mode="formatted")` (plus lisible)
  ou `df.explain(True)` (plans logique et physique).

### À quoi repérer dans le plan

- `FileScan parquet` : la lecture du fichier. Regardez `ReadSchema` et `PushedFilters` (les
  filtres poussés jusqu'à la lecture).
- `Exchange hashpartitioning(...)` : un shuffle, déclenché par le `groupBy` / `GROUP BY`.
- `HashAggregate` : l'agrégation, en deux temps (partiel par partition, puis final après le
  shuffle).

### Rappels SQL utiles

```sql
-- Filtrer puis projeter
SELECT colA, colB FROM courses WHERE payment_type = 1 AND total_amount > 0;

-- Agreger
SELECT PULocationID, COUNT(*) AS nb, SUM(total_amount) AS revenu
FROM courses GROUP BY PULocationID ORDER BY revenu DESC;

-- Jointure
SELECT z.Zone, a.nb FROM agregat AS a LEFT JOIN zones AS z ON a.PULocationID = z.LocationID;

-- Fonctions de date
SELECT HOUR(tpep_pickup_datetime), DAYOFWEEK(tpep_pickup_datetime) FROM courses;
```

### Si quelque chose coince

- **`Table or view not found`** : la vue n'a pas été créée, ou le nom ne correspond pas (la
  casse compte). Relancez `createOrReplaceTempView` et vérifiez avec `SHOW TABLES`.
- **`AnalysisException: cannot resolve column`** : nom de colonne mal orthographié. Attention,
  les colonnes taxi ont une casse précise (`PULocationID`, pas `pulocationid`).
- **Les deux comptes diffèrent à la consigne 2** : vérifiez que la condition SQL est bien
  `payment_type = 1 AND total_amount > 0` (le `AND`, pas un `OR`).
- **Les plans semblent un peu différents** : c'est normal s'il reste un détail d'ordre ou
  d'alias. L'essentiel est que les mêmes opérations physiques (scan, exchange, aggregate)
  apparaissent dans le même ordre.
