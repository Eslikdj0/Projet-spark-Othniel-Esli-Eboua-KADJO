# Optimiser un traitement : broadcast join, cache et partitionnement

## Objectif

Mesurer concrètement l'effet de trois optimisations Spark et apprendre à les justifier avec des
chiffres, pas avec des impressions. Vous allez :

- chronométrer une jointure classique (avec shuffle) entre les courses et la table des zones ;
- la rejouer en `broadcast join` et mesurer le gain ;
- mettre en cache un DataFrame réutilisé plusieurs fois et comparer les temps ;
- comparer `repartition` (avec shuffle) et `coalesce` (sans shuffle) sur le nombre de partitions.

À la fin de ce TP, vous savez prendre une mesure avant / après, lire le plan d'exécution pour
confirmer ce qui change, et choisir la bonne optimisation selon la situation.

## Contexte

On reste sur le fil rouge : les courses de taxi jaunes de New York (NYC TLC), mois de janvier
2024, fichier `yellow_tripdata_2024-01.parquet` (environ 3 millions de courses). La table de
correspondance `taxi_zone_lookup.csv` associe chaque `LocationID` à un quartier (`Borough`) et à
un nom de zone (`Zone`). Elle est minuscule : 265 lignes.

Cette asymétrie est exactement le terrain de jeu du `broadcast join` : quand une table est petite,
Spark peut la diffuser (broadcast) sur tous les executors pour éviter le shuffle de la grosse
table. On va le mesurer.

On suppose les deux fichiers présents dans `data/datasets/` :

```
data/datasets/yellow_tripdata_2024-01.parquet
data/datasets/taxi_zone_lookup.csv
```

Colonnes utiles : `tpep_pickup_datetime`, `trip_distance`, `PULocationID`, `DOLocationID`,
`fare_amount`, `tip_amount`, `total_amount`, `payment_type`.

> Important sur la mesure : en mode local, les temps varient d'un run à l'autre (cache du système
> de fichiers, JIT de la JVM, charge de la machine). On ne cherche pas une valeur exacte mais un
> ordre de grandeur et une tendance reproductible. Lancez chaque mesure deux ou trois fois.

## Consignes

Créez un fichier `tp08_optimisation.py`. Démarrez une SparkSession en local et préparez un petit
utilitaire de chronométrage. On force une action (`count()`) pour déclencher réellement le calcul,
car tant qu'aucune action n'est appelée, rien ne s'exécute (évaluation paresseuse) :

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP08 - Optimisation")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def chrono(label, fonction):
    """Mesure le temps d'exécution d'une fonction et renvoie son résultat."""
    debut = time.perf_counter()
    resultat = fonction()
    duree = time.perf_counter() - debut
    print(f"[{label}] {duree:.2f} s")
    return resultat

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
)
```

### 1. Mesurer une jointure classique

On veut, par quartier de départ (`Borough`), le nombre de courses et le revenu total. On joint
les courses à la table des zones sur `PULocationID = LocationID`, puis on agrège.

Pour cette première mesure, on neutralise une optimisation que Spark fait tout seul : la
conversion automatique en broadcast quand une table est petite. On la désactive avec
`spark.sql.autoBroadcastJoinThreshold = -1` pour observer le vrai shuffle.

```python
# Desactiver le broadcast automatique pour forcer un sort-merge join (avec shuffle)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def join_classique():
    return (
        courses
        .join(zones, courses.PULocationID == zones.LocationID, "left")
        .groupBy("Borough")
        .agg(
            F.count("*").alias("nb_courses"),
            F.round(F.sum("total_amount"), 2).alias("revenu_total"),
        )
        .count()  # action : declenche le calcul
    )

chrono("join classique (shuffle)", join_classique)
```

À compléter : affichez le plan physique de cette jointure et repérez l'opérateur de jointure et
le ou les `Exchange` (les shuffles) :

```python
plan_classique = (
    courses
    .join(zones, courses.PULocationID == zones.LocationID, "left")
    .groupBy("Borough")
    .agg(F.count("*").alias("nb_courses"))
)
plan_classique.explain()
# Question : quel type de jointure voyez-vous ? Combien d'Exchange ?
```

### 2. Passer en broadcast join

Maintenant, demandez explicitement à Spark de diffuser la petite table avec `F.broadcast`.
L'idée : la table des zones (265 lignes) est envoyée entière à chaque executor, donc plus besoin
de shuffler les 3 millions de courses pour la jointure.

```python
from pyspark.sql.functions import broadcast

def join_broadcast():
    return (
        courses
        .join(broadcast(zones), courses.PULocationID == zones.LocationID, "left")
        .groupBy("Borough")
        .agg(
            F.count("*").alias("nb_courses"),
            F.round(F.sum("total_amount"), 2).alias("revenu_total"),
        )
        .count()
    )

chrono("join broadcast", join_broadcast)
```

À compléter : affichez le plan de la version broadcast et comparez-le à celui de la consigne 1.
L'opérateur de jointure doit avoir changé, et un `Exchange` doit avoir disparu :

```python
plan_broadcast = (
    courses
    .join(broadcast(zones), courses.PULocationID == zones.LocationID, "left")
    .groupBy("Borough")
    .agg(F.count("*").alias("nb_courses"))
)
plan_broadcast.explain()
# Question : quel operateur de jointure remplace le sort-merge join ?
```

Notez les deux temps (consigne 1 contre consigne 2) et le rapport entre les deux.

### 3. Cacher un DataFrame réutilisé

On construit un DataFrame de travail enrichi (durée de course, prix au km, filtrage des aberrations)
puis on l'interroge **plusieurs fois**. Sans cache, Spark recalcule tout depuis la lecture du
Parquet à chaque action. Avec cache, il garde le résultat en mémoire après la première action.

```python
travail = (
    courses
    .filter((F.col("trip_distance") > 0) & (F.col("total_amount") > 0))
    .withColumn(
        "duree_min",
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60,
    )
    .withColumn("prix_par_km", F.round(F.col("fare_amount") / (F.col("trip_distance") * 1.60934), 2))
)

# Sans cache : trois actions, donc trois recalculs complets
def trois_actions():
    a = travail.count()
    b = travail.agg(F.avg("duree_min")).collect()
    c = travail.agg(F.avg("prix_par_km")).collect()
    return a

chrono("3 actions SANS cache", trois_actions)

# A completer : mettre en cache, forcer le calcul une fois, puis remesurer
travail.cache()
travail.count()  # premiere action : remplit le cache (a ne PAS chronometrer)

chrono("3 actions AVEC cache", trois_actions)

travail.unpersist()  # liberer la memoire quand on a fini
```

Notez les deux temps. Le second doit être nettement plus court.

### 4. Comparer repartition et coalesce

`repartition(n)` change le nombre de partitions en redistribuant les données (shuffle complet,
peut augmenter ou diminuer le nombre). `coalesce(n)` ne fait que regrouper des partitions
existantes (pas de shuffle, peut seulement diminuer). On va le constater.

```python
print("Partitions au depart :", courses.rdd.getNumPartitions())

def via_repartition():
    return courses.repartition(8).count()

def via_coalesce():
    return courses.coalesce(2).count()

chrono("repartition(8)", via_repartition)
chrono("coalesce(2)", via_coalesce)
```

À compléter : affichez le plan des deux versions et repérez la différence. La version
`repartition` doit contenir un `Exchange` (shuffle), pas la version `coalesce` :

```python
courses.repartition(8).explain()
courses.coalesce(2).explain()
# Question : laquelle contient un Exchange ? Pourquoi l'autre n'en a pas besoin ?
```

### 5. Conclure

Dans un commentaire en fin de fichier, répondez en quelques lignes :

- Combien de fois le broadcast join a-t-il accéléré la jointure (rapport des temps) ?
- Le cache a-t-il bien réduit le temps des trois actions ? De combien environ ?
- Pourquoi `coalesce` n'a-t-il pas besoin de shuffle, contrairement à `repartition` ?

## Livrable

Vous avez réussi le TP si :

- Vous avez **quatre mesures de temps** affichées : join classique, join broadcast, 3 actions sans
  cache, 3 actions avec cache.
- Le **broadcast join est plus rapide** que la jointure classique, et son plan ne contient plus le
  `Exchange` lié à la jointure (l'opérateur est devenu un `BroadcastHashJoin`).
- Les **trois actions avec cache sont nettement plus rapides** que sans cache (typiquement un
  facteur 2 ou plus).
- Vous pouvez montrer, plan à l'appui, que **`repartition` déclenche un `Exchange`** alors que
  **`coalesce` n'en déclenche pas**.
- Votre conclusion (consigne 5) répond aux trois questions avec des chiffres tirés de vos mesures.

## Aide

### Rappels d'API

- Forcer une action pour mesurer : `df.count()` parcourt tout. Évitez `df.collect()` sur un gros
  DataFrame (ramène tout sur le driver). Pour mesurer une agrégation, `.collect()` sur le petit
  résultat agrégé est correct.
- Broadcast explicite : `from pyspark.sql.functions import broadcast` puis
  `gros.join(broadcast(petit), cond, "left")`.
- Seuil de broadcast automatique : `spark.sql.autoBroadcastJoinThreshold` (10 Mo par défaut). Le
  mettre à `-1` désactive la conversion automatique ; c'est utile pour **forcer** le shuffle et
  comparer. En production, on laisse Spark décider.
- Cache : `df.cache()` (équivaut à `persist(MEMORY_AND_DISK)`). Le cache se remplit à la
  **première action** après l'appel, pas à l'appel lui-même (lazy). Libérez avec `df.unpersist()`.
- Nombre de partitions : `df.rdd.getNumPartitions()`. Changer : `df.repartition(n)` (shuffle) ou
  `df.coalesce(n)` (sans shuffle, diminue seulement).
- Plan d'exécution : `df.explain()` (physique) ou `df.explain(mode="formatted")` (plus lisible).

### À quoi repérer dans les plans

- `SortMergeJoin` : la jointure classique, précédée de deux `Exchange` (un par côté) et de tris.
  C'est ce qu'on veut éviter quand un côté est petit.
- `BroadcastHashJoin` + `BroadcastExchange` : la version broadcast. La petite table est diffusée,
  la grosse n'est plus shufflée pour la jointure.
- `Exchange hashpartitioning(...)` : un shuffle. Il apparaît pour `repartition` et pour le
  `groupBy`, mais pas pour `coalesce`.
- `InMemoryTableScan` : apparaît quand on lit un DataFrame mis en cache (au lieu de relire le
  Parquet).

### Mesurer proprement

- Lancez chaque mesure deux ou trois fois et gardez la valeur basse (la première paie le coût du
  démarrage de la JVM et du remplissage des caches du système).
- Ne chronométrez **jamais** la construction du DataFrame seule : sans action, Spark ne fait rien
  (évaluation paresseuse). C'est pour cela que `chrono(...)` appelle une fonction qui se termine
  par `.count()` ou `.collect()`.
- Ouvrez la Spark UI (http://localhost:4040) pendant le run : l'onglet SQL montre le DAG de chaque
  requête, l'onglet Storage montre le DataFrame en cache.

### Si quelque chose coince

- **Aucun gain visible avec le broadcast** : vérifiez que vous avez bien désactivé le broadcast
  automatique à la consigne 1 (sinon Spark broadcastait déjà la table des zones, et les deux
  versions sont identiques). C'est le piège le plus courant de ce TP.
- **`coalesce(2)` ne change pas le temps** : c'est possible si le fichier ne tient déjà que sur
  peu de partitions ; regardez `getNumPartitions()` avant et après.
- **Le cache ne semble rien changer** : assurez-vous d'avoir appelé une action *après* `cache()`
  pour le remplir (la ligne `travail.count()` non chronométrée), et de ne pas avoir recréé le
  DataFrame `travail` entre-temps.
- **`Py4JJavaError` de mémoire** : réduisez le volume (un seul mois suffit) ou baissez le
  parallélisme avec `.master("local[2]")`.
