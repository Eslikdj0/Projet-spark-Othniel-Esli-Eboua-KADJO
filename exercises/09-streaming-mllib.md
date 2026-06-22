# BONUS : mini Structured Streaming ou mini pipeline MLlib

## Objectif

Découvrir, au choix, l'une des deux ouvertures du jour 3 :

- **Parcours A - Structured Streaming** : surveiller un dossier de fichiers, traiter chaque
  nouveau fichier comme un micro-batch avec `readStream` / `writeStream` et un sink console, et
  constater que c'est le même code DataFrame qu'en batch, mais sur un flux qui ne se termine pas.
- **Parcours B - MLlib** : construire un mini pipeline de machine learning
  (`VectorAssembler` + un estimateur simple) pour prédire le pourboire d'une course de taxi, en
  passant par `fit` / `transform` et une évaluation honnête sur un jeu de test.

C'est un TP bonus : choisissez **un seul** parcours (ou les deux si vous avez le temps). Le but
n'est pas la performance ni l'exhaustivité, mais de poser les premiers gestes de chaque domaine.

## Contexte

On reste sur le fil rouge des courses de taxi jaunes de New York (NYC TLC). Le fichier
`yellow_tripdata_2024-01.parquet` contient environ 3 millions de courses pour janvier 2024, et la
table des zones `taxi_zone_lookup.csv` donne le nom de chaque zone à partir de son identifiant
(`LocationID`). On suppose les deux fichiers présents dans `data/datasets/`.

Colonnes utiles : `tpep_pickup_datetime`, `trip_distance`, `passenger_count`, `PULocationID`,
`fare_amount`, `tip_amount`, `total_amount`, `payment_type`.

Rappel de cours :

- En streaming (J3.12), Spark applique le modèle micro-batch : un flux est une suite de petits
  batchs, et on réutilise l'API DataFrame habituelle. Une source de type fichier surveille un
  dossier ; un sink console réaffiche le résultat à chaque batch.
- En MLlib (J3.13), un pipeline enchaîne des étapes : un `VectorAssembler` rassemble les colonnes
  d'entrée en un seul vecteur `features`, puis un estimateur (ici une régression) apprend avec
  `fit` et prédit avec `transform`.

---

## Parcours A - Structured Streaming

### A.1 Préparer un réservoir de fichiers à streamer

Une source de streaming "fichier" surveille un dossier vide au départ : il faut donc des fichiers
à y déposer au fil de l'eau. Créez un fichier `tp09a_streaming.py`. Commencez par découper le
Parquet taxi en plusieurs petits fichiers rangés dans un dossier "réservoir", en gardant le schéma :

```python
import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

STREAM_IN = "data/datasets/tp09_stream_in"     # dossier surveille par le flux (vide au depart)
PARTS_DIR = "data/datasets/tp09_stream_parts"  # reservoir de fichiers a deposer
CHECKPOINT = "data/datasets/tp09_stream_ckpt"  # etat du flux (offsets, agregats)
NB_LOTS = 6

spark = (
    SparkSession.builder
    .appName("TP09A - Structured Streaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")  # petite demo, evitons 200 taches
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# On repart d'un etat propre
for d in (STREAM_IN, PARTS_DIR, CHECKPOINT):
    shutil.rmtree(d, ignore_errors=True)
os.makedirs(STREAM_IN, exist_ok=True)

courses = (
    spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
    .filter(F.col("fare_amount") > 0)
    .select("PULocationID", "total_amount", "tpep_pickup_datetime")
    .limit(120000)            # on reduit le volume pour une demo rapide
    .repartition(NB_LOTS)     # NB_LOTS partitions = NB_LOTS fichiers
)
courses.write.mode("overwrite").parquet(PARTS_DIR)

# A completer : recuperer le schema du reservoir (obligatoire pour la source streaming)
schema = spark.read.parquet(________).schema
```

### A.2 Définir le flux avec `readStream`

`readStream` a la même API que `read`, mais en mode flux. Déclarez le schéma (obligatoire pour une
source fichier) et lisez un seul fichier par micro-batch pour bien voir l'effet :

```python
flux = (
    spark.readStream
    .schema(________)                       # schema recupere en A.1
    .option("maxFilesPerTrigger", 1)        # un fichier = un micro-batch
    .parquet(________)                      # quel dossier surveille-t-on ?
)
```

### A.3 Transformer le flux comme un DataFrame batch

C'est le message clé : le code de transformation est identique à du batch. Joignez la petite table
des zones (lue en batch classique) puis agrégez le nombre de courses et le revenu cumulé par
arrondissement (`Borough`) :

```python
zones = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
    .select("LocationID", "Borough")
)

agrege = (
    flux.join(F.broadcast(zones), flux["PULocationID"] == zones["LocationID"], "left")
    .groupBy("________")          # par arrondissement
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu_cumule"),
    )
)
```

### A.4 Démarrer le flux avec `writeStream` (sink console)

Une agrégation sans watermark impose le mode de sortie `complete` : on réaffiche tout le tableau à
chaque batch. Le sink est la console :

```python
requete = (
    agrege.writeStream
    .outputMode("________")                 # complete, append ou update ?
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)
```

### A.5 Simuler l'arrivée des données

Le dossier surveillé est vide : déposez-y les fichiers du réservoir un par un, et observez le
tableau agrégé se mettre à jour à chaque dépôt :

```python
fichiers = [f for f in sorted(os.listdir(PARTS_DIR)) if f.endswith(".parquet")]
for i, nom in enumerate(fichiers, start=1):
    time.sleep(6)  # laisser le batch precedent s'afficher
    shutil.copy(os.path.join(PARTS_DIR, nom), os.path.join(STREAM_IN, f"batch_{i:02d}.parquet"))
    print(f"  >>> Depose le fichier {i}/{len(fichiers)}")

requete.awaitTermination(timeout=15)
requete.stop()
spark.stop()
```

Question à noter : le `nb_courses` total et le `revenu_cumule` augmentent-ils à chaque batch ?
Pourquoi le tableau est-il réaffiché en entier à chaque fois (et pas seulement les nouvelles lignes) ?

---

## Parcours B - MLlib : prédire le pourboire

### B.1 Charger et nettoyer les données

Créez un fichier `tp09b_mllib.py`. Un modèle n'apprend rien de bon sur du bruit : sélectionnez les
colonnes utiles et filtrez les valeurs aberrantes (montants négatifs, distances absurdes) :

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = (
    SparkSession.builder
    .appName("TP09B - MLlib pourboire")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

donnees = (
    spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
    .select("trip_distance", "fare_amount", "passenger_count", "payment_type", "tip_amount")
    .filter(
        (F.col("fare_amount") > 0) & (F.col("fare_amount") < 200)
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") < 100)
        & (F.col("tip_amount") >= 0) & (F.col("tip_amount") < 100)
        & (F.col("passenger_count") >= 1)
    )
    .na.drop()
)
print("Lignes utilisables :", donnees.count())
```

### B.2 Découper apprentissage et test

On entraîne sur une partie des données, on évalue sur une autre que le modèle n'a jamais vue. Sans
ce découpage, on triche : on noterait le modèle sur ce qu'il a déjà appris.

```python
train, test = donnees.randomSplit([0.8, 0.2], seed=42)
print("train :", train.count(), "| test :", test.count())
```

### B.3 Assembler les features

MLlib travaille sur une seule colonne vectorielle. Rassemblez les colonnes d'entrée avec un
`VectorAssembler` dans une colonne `features` :

```python
colonnes_entree = ["trip_distance", "fare_amount", "passenger_count", "payment_type"]
assembleur = VectorAssembler(
    inputCols=________,        # quelles colonnes en entree ?
    outputCol="features",
)
```

### B.4 Entraîner le modèle (`fit`)

Définissez la régression (cible = `tip_amount`), assemblez les features sur le jeu d'apprentissage,
puis appelez `fit`. C'est l'étape qui déclenche le calcul :

```python
regression = LinearRegression(
    featuresCol="features",
    labelCol="________",       # que cherche-t-on a predire ?
    predictionCol="prediction",
)

train_features = assembleur.transform(train)
modele = regression.fit(train_features)

print("Coefficients :", modele.coefficients)
print("Ordonnee a l'origine :", round(modele.intercept, 4))
```

> Astuce : si vous préférez, encapsulez `assembleur` et `regression` dans un
> `Pipeline(stages=[...])` et appelez `pipeline.fit(train)`. Le résultat est identique, mais le
> pipeline enchaîne les étapes pour vous (pas besoin du `transform` manuel).

### B.5 Prédire (`transform`) et évaluer

Appliquez le modèle au jeu de test, comparez pourboire réel et prédit, puis mesurez l'erreur avec
RMSE et R2 :

```python
test_features = assembleur.transform(test)
predictions = modele.transform(test_features)

predictions.select(
    "trip_distance", "fare_amount", "payment_type",
    F.round("tip_amount", 2).alias("pourboire_reel"),
    F.round("prediction", 2).alias("pourboire_predit"),
).show(10, truncate=False)

rmse = RegressionEvaluator(
    labelCol="tip_amount", predictionCol="prediction", metricName="rmse"
).evaluate(predictions)
r2 = RegressionEvaluator(
    labelCol="tip_amount", predictionCol="prediction", metricName="r2"
).evaluate(predictions)
print("RMSE :", round(rmse, 3), "| R2 :", round(r2, 3))

spark.stop()
```

Question à noter : quel coefficient est le plus fort ? Que vous apprend-il sur le pourboire et le
mode de paiement ?

## Livrable

Vous avez réussi le TP si, pour le parcours choisi :

**Parcours A (streaming)**

- Le flux démarre sans erreur et affiche un Batch 0, puis un nouveau batch à chaque fichier déposé.
- Le tableau par `Borough` se met à jour à chaque batch, et `nb_courses` augmente d'un dépôt à l'autre.
- Vous pouvez expliquer en une phrase pourquoi le mode `complete` réaffiche tout le tableau, et
  pourquoi la requête ne se termine pas toute seule (il faut `stop` ou un timeout).

**Parcours B (MLlib)**

- Le modèle s'entraîne (`fit`) et affiche ses coefficients et son ordonnée à l'origine.
- Les prédictions sortent une colonne `pourboire_predit` à côté du `pourboire_reel` sur 10 courses.
- Le RMSE et le R2 s'affichent. Vous savez dire approximativement de combien de dollars le modèle
  se trompe en moyenne (RMSE) et quelle part de la variance il explique (R2).
- Vous pouvez commenter le poids de `payment_type` dans les coefficients.

## Aide

### Parcours A - rappels Structured Streaming

- `spark.readStream.schema(...).parquet(dossier)` : une source fichier doit connaître son schéma à
  l'avance (le dossier peut être vide au démarrage, l'inférence est impossible). D'où le
  `spark.read.parquet(reservoir).schema` récupéré en A.1.
- `maxFilesPerTrigger=1` : un fichier par micro-batch, pour bien voir l'effet pas à pas. Sans cette
  option, Spark avalerait tous les fichiers présents d'un coup.
- Mode de sortie : pour une agrégation **sans watermark**, seul `complete` est autorisé (Spark doit
  garder tout l'état). `append` sert plutôt à un flux sans agrégation, `update` n'écrit que les
  lignes modifiées.
- `checkpointLocation` est obligatoire pour un sink avec état : c'est là que Spark range les offsets
  et l'agrégat. Si vous relancez le script, supprimez d'abord ce dossier (le code le fait déjà).
- La requête tourne en arrière-plan après `start()`. Pour l'arrêter proprement :
  `requete.awaitTermination(timeout=15)` puis `requete.stop()`.

### Parcours B - rappels MLlib

- Imports : `from pyspark.ml.feature import VectorAssembler`,
  `from pyspark.ml.regression import LinearRegression`,
  `from pyspark.ml.evaluation import RegressionEvaluator`.
- `VectorAssembler(inputCols=[...], outputCol="features")` crée une colonne vecteur. MLlib exige une
  unique colonne de features, pas plusieurs colonnes séparées.
- `randomSplit([0.8, 0.2], seed=42)` : le `seed` rend le découpage reproductible (mêmes lignes à
  chaque exécution).
- `fit` rend un modèle (un transformer). `transform` ajoute la colonne `prediction`. C'est le même
  verbe `transform` que sur un DataFrame.
- `RegressionEvaluator(metricName="rmse")` donne l'erreur en dollars ; `metricName="r2"` donne la
  part de variance expliquée (entre 0 et 1, plus c'est haut mieux c'est).

### Si quelque chose coince

- **Parcours A, rien ne s'affiche** : vérifiez que les fichiers sont bien copiés DANS `STREAM_IN`
  (pas dans le réservoir), et laissez passer quelques secondes (le `time.sleep`). Un `Batch 0` vide
  au démarrage est normal.
- **`Queries with streaming sources must be executed with writeStream.start()`** : vous avez appelé
  `show()` ou `count()` sur un DataFrame de streaming. Sur un flux, on passe par
  `writeStream...start()`, pas par les actions batch.
- **Parcours B, `Column tip_amount must be of type Double`** : assurez-vous que `labelCol` et les
  `inputCols` sont bien numériques. En cas de doute, un `printSchema()` après le `select` le confirme.
- **R2 très faible ou négatif** : c'est attendu et même intéressant. La régression linéaire sur ces
  quelques colonnes est volontairement simpliste. Le but est le pipeline, pas la performance ; des colonnes plus informatives amélioreraient le score.
