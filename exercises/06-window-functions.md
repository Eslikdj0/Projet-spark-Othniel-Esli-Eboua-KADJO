# Window functions : classer et analyser l'évolution des courses de taxi

## Objectif

Maîtriser les window functions de Spark : définir une fenêtre avec `Window.partitionBy(...)` et
`Window.orderBy(...)`, puis appliquer `row_number`, `rank`, `lag` et une moyenne glissante. À la
différence d'un `groupBy`, une window function calcule une valeur **par ligne** sans réduire le
nombre de lignes : on garde le détail tout en ajoutant un classement, un rang, une comparaison avec
la ligne précédente ou une moyenne mobile.

À la fin de ce TP, vous savez répondre à des questions du type : quelles sont les courses les plus
chères de chaque zone, comment évolue le revenu jour après jour, et comment lisser une série
temporelle bruitée avec une moyenne glissante sur 7 jours.

## Contexte

On reste sur le fil rouge : les courses de taxi jaunes de New York (NYC TLC), mois de janvier
2024, fichier `yellow_tripdata_2024-01.parquet` (environ 3 millions de courses). La table des zones
`taxi_zone_lookup.csv` permet de remplacer les identifiants numériques par des noms lisibles
(quartier `Borough`, zone `Zone`). On suppose les deux fichiers présents dans `data/datasets/` :

```
data/datasets/yellow_tripdata_2024-01.parquet
data/datasets/taxi_zone_lookup.csv
```

Colonnes utiles : `tpep_pickup_datetime`, `trip_distance`, `PULocationID`, `fare_amount`,
`tip_amount`, `total_amount`, `payment_type`.

Pourquoi les window functions ? Avec un `groupBy("PULocationID")`, on obtient une ligne par zone et
on perd le détail des courses. Si la question est "donne-moi les 3 courses les plus chères **de
chaque zone**", il faut classer les lignes **à l'intérieur de chaque groupe** sans les écraser :
c'est exactement le rôle d'une window function.

## Consignes

Créez un fichier `tp06_window_functions.py`. Démarrez une SparkSession en local, chargez le Parquet
et nettoyez rapidement les courses aberrantes (elles fausseraient les classements) :

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("TP06 - Window functions")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")

# Nettoyage minimal : montant et distance coherents, janvier 2024 uniquement
courses = courses.filter(
    (F.col("total_amount") > 0) &
    (F.col("trip_distance") > 0) &
    (F.col("tpep_pickup_datetime") >= "2024-01-01") &
    (F.col("tpep_pickup_datetime") < "2024-02-01")
)
```

### 1. Classer les courses dans chaque zone avec row_number

On veut, pour chaque zone de départ (`PULocationID`), numéroter les courses de la plus chère à la
moins chère selon `total_amount`. Définissez une fenêtre partitionnée par zone et ordonnée par
montant décroissant, puis ajoutez une colonne `rang` avec `F.row_number()` :

```python
fenetre_zone = Window.partitionBy("PULocationID").orderBy(F.col("total_amount").desc())

# A completer : ajouter la colonne "rang" avec row_number sur cette fenetre
courses_classees = courses.withColumn("rang", F.row_number().over(________))

# Garder les 3 courses les plus cheres de chaque zone
top3_par_zone = courses_classees.filter(F.col("rang") <= 3)
top3_par_zone.select(
    "PULocationID", "rang", "trip_distance", "total_amount"
).orderBy("PULocationID", "rang").show(15, truncate=False)
```

### 2. row_number contre rank contre dense_rank

Les trois fonctions numérotent, mais gèrent les ex aequo différemment. Ajoutez les trois sur la
même fenêtre et observez la différence sur une seule zone (par exemple `PULocationID = 132`,
l'aéroport JFK) :

```python
courses_trois_rangs = (
    courses
    .withColumn("row_number", F.row_number().over(fenetre_zone))
    .withColumn("rank", F.rank().over(fenetre_zone))
    # A completer : ajouter "dense_rank" avec F.dense_rank()
    .withColumn("dense_rank", ________)
)

(courses_trois_rangs
 .filter(F.col("PULocationID") == 132)
 .select("total_amount", "row_number", "rank", "dense_rank")
 .show(20, truncate=False))
```

Notez ce que vous observez : que se passe-t-il pour deux courses au **même** `total_amount` ?

### 3. Construire une série temporelle journalière

Pour les questions d'évolution (consignes 4 et 5), on a besoin d'un revenu **par jour**. Agrégez les
courses par date de prise en charge :

```python
revenu_jour = (
    courses
    .withColumn("jour", F.to_date("tpep_pickup_datetime"))
    .groupBy("jour")
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu"),
    )
    .orderBy("jour")
)
revenu_jour.show(31, truncate=False)
```

Vous devez obtenir une trentaine de lignes (les jours de janvier 2024).

### 4. Comparer chaque jour au précédent avec lag

`F.lag(colonne, n)` renvoie la valeur de la colonne `n` lignes plus haut dans la fenêtre. On l'utilise
pour comparer le revenu d'un jour à celui de la veille. Définissez une fenêtre **globale** ordonnée par
jour (sans `partitionBy`, car on a une seule série), puis calculez la variation :

```python
fenetre_temps = Window.orderBy("jour")

revenu_variation = (
    revenu_jour
    # A completer : revenu de la veille avec F.lag("revenu", 1)
    .withColumn("revenu_veille", ________)
    .withColumn("variation", F.col("revenu") - F.col("revenu_veille"))
    .withColumn(
        "variation_pct",
        F.round(100 * (F.col("revenu") - F.col("revenu_veille")) / F.col("revenu_veille"), 1)
    )
)
revenu_variation.show(31, truncate=False)
```

Repérez les chutes de revenu : à quel(s) jour(s) de la semaine correspondent-elles ?

### 5. Lisser avec une moyenne glissante sur 7 jours

Une série journalière est bruitée (effet week-end, jours fériés). Une moyenne glissante sur 7 jours
lisse ces variations. Une window function avec un cadre (`rowsBetween`) calcule la moyenne sur la
ligne courante et les 6 précédentes :

```python
# Fenetre des 7 derniers jours : la ligne courante et les 6 lignes precedentes
fenetre_7j = Window.orderBy("jour").rowsBetween(-6, 0)

revenu_lisse = revenu_variation.withColumn(
    "moyenne_7j",
    # A completer : moyenne glissante du revenu sur cette fenetre
    F.round(________, 2)
)
revenu_lisse.select("jour", "revenu", "moyenne_7j").show(31, truncate=False)
```

Comparez la colonne `revenu` (en dents de scie) et `moyenne_7j` (lissée). La moyenne glissante doit
gommer les creux du week-end.

### 6. Combiner partitionBy et une window pour un classement enrichi

Question métier finale : pour chaque zone de départ, quel est le rang d'une course **et** sa part
dans le revenu total de la zone ? On combine une window de classement et une window d'agrégation
(somme sur toute la partition, sans `orderBy`), puis on joint la table des zones pour un affichage
lisible :

```python
zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
)

# Window d'agregation : somme sur toute la zone (pas de orderBy, donc toute la partition)
fenetre_total_zone = Window.partitionBy("PULocationID")

resultat = (
    courses_classees                                   # contient deja "rang"
    .withColumn("revenu_zone", F.sum("total_amount").over(fenetre_total_zone))
    .withColumn("part_course", F.round(100 * F.col("total_amount") / F.col("revenu_zone"), 3))
    .filter(F.col("rang") <= 3)
    .join(zones, F.col("PULocationID") == zones.LocationID, "left")
    .select("Borough", "Zone", "rang", "total_amount", "part_course")
    .orderBy("Zone", "rang")
)
resultat.show(20, truncate=False)
```

N'oubliez pas `spark.stop()` à la fin.

## Livrable

Vous avez réussi le TP si :

- Avec `row_number` (consigne 1), `top3_par_zone` contient exactement 3 lignes par zone (rang 1, 2,
  3), classées du montant le plus élevé au plus faible.
- La comparaison `row_number` / `rank` / `dense_rank` (consigne 2) montre clairement la différence :
  sur des ex aequo, `row_number` continue 1, 2, 3, 4, tandis que `rank` saute des valeurs après une
  égalité (1, 1, 3) et `dense_rank` n'en saute pas (1, 1, 2).
- La série journalière (consigne 3) compte une trentaine de lignes (les jours de janvier).
- Avec `lag` (consigne 4), la première ligne a un `revenu_veille` nul (pas de jour avant le 1er
  janvier), et les variations en pourcentage sont cohérentes (chutes nettes le week-end et le 1er
  janvier, hausses en semaine).
- La moyenne glissante (consigne 5) produit une colonne `moyenne_7j` visiblement plus lisse que
  `revenu` : les pics et les creux sont atténués.
- Le résultat final (consigne 6) affiche des noms de zones lisibles, le rang de chaque course et sa
  part en pourcentage dans le revenu de sa zone.

## Aide

### Le modèle mental d'une window function

Une window function se lit en trois temps :

1. `partitionBy(...)` découpe les lignes en groupes (comme un `GROUP BY`), mais **sans les fusionner**.
2. `orderBy(...)` ordonne les lignes à l'intérieur de chaque groupe (indispensable pour `row_number`,
   `rank`, `lag` et les moyennes glissantes).
3. La fonction (`row_number`, `rank`, `lag`, `avg`, `sum`...) est appliquée avec `.over(fenetre)`,
   et renvoie **une valeur par ligne**. Le nombre de lignes ne change pas.

```python
from pyspark.sql.window import Window

fenetre = Window.partitionBy("col_groupe").orderBy(F.col("col_tri").desc())
df = df.withColumn("rang", F.row_number().over(fenetre))
```

### Rappels d'API

- `F.row_number()` : numérotation stricte 1, 2, 3, 4... Pas d'ex aequo, deux lignes égales reçoivent
  des numéros différents (l'ordre entre elles n'est pas garanti).
- `F.rank()` : même rang pour les ex aequo, puis **saut** (1, 1, 3, 4).
- `F.dense_rank()` : même rang pour les ex aequo, **sans saut** (1, 1, 2, 3).
- `F.lag("col", n)` : valeur n lignes avant la ligne courante (None si on est au début). `F.lead("col", n)`
  fait l'inverse (n lignes après).
- Moyenne glissante : `F.avg("col").over(fenetre.rowsBetween(-6, 0))`. Le cadre `rowsBetween(debut, fin)`
  est compté en lignes relatives à la ligne courante : `-6` = 6 lignes avant, `0` = ligne courante.
- `Window.partitionBy(...)` **sans** `orderBy` : la fonction (par exemple `F.sum`) s'applique à toute
  la partition. Utile pour calculer un total de groupe à remettre sur chaque ligne (consigne 6).

### Commandes utiles

```bash
# Verifier la presence des fichiers
ls -lh data/datasets/yellow_tripdata_2024-01.parquet data/datasets/taxi_zone_lookup.csv
```

### Bornes de fenêtre : rowsBetween contre rangeBetween

- `rowsBetween(-6, 0)` : un nombre fixe de **lignes** (les 6 précédentes plus la courante). C'est ce
  qu'on veut pour une moyenne sur 7 jours **si la série n'a pas de trous** (un jour par ligne).
- `rangeBetween(...)` : un intervalle de **valeurs** de la colonne de tri, pas un nombre de lignes.
  Plus robuste si des jours manquent, mais plus subtil à manier. On reste sur `rowsBetween` ici.

### Si quelque chose coince

- **`row_number` exige un `orderBy`** : `F.row_number().over(Window.partitionBy("x"))` sans `orderBy`
  lève une erreur. Les fonctions de classement ont besoin d'un ordre. En revanche `F.sum().over(...)`
  fonctionne avec ou sans `orderBy`.
- **Une window function sans `partitionBy` traite tout dans une seule partition** : pour une petite
  série (30 lignes journalières) c'est sans danger, Spark prévient juste avec un avertissement "No
  Partition Defined for Window operation". Ne jamais faire ça sur les 3 millions de courses brutes.
- **Ne pas confondre `groupBy` et `partitionBy`** : `groupBy` réduit (une ligne par groupe),
  `partitionBy` garde toutes les lignes et ajoute une colonne calculée. Pour le top 3 par zone, on a
  besoin du détail des courses, donc d'une window, pas d'un `groupBy`.
- **`lag` sur la première ligne renvoie None** : c'est normal (il n'y a pas de veille au 1er janvier).
  Le calcul de `variation_pct` produira alors un `null`, pas une erreur.
