# Lire la Spark UI et le plan d'exécution

## Objectif

Comprendre comment Spark exécute réellement un traitement. Vous allez lancer un job qui contient
un `groupBy` puis une jointure (donc un shuffle), ouvrir la Spark UI, lire le DAG et les stages,
puis interpréter le plan d'exécution renvoyé par `df.explain(True)`. Le fil conducteur est de
repérer l'`Exchange` : c'est le nom que Spark donne au shuffle dans le plan physique et dans le
graphe d'exécution.

A la fin de ce TP, vous savez relier trois choses qui parlent du même phénomène : une
transformation large dans votre code Python, un `Exchange` dans le plan d'exécution, et un
découpage en stages dans la Spark UI.

## Contexte

On reste sur le fil rouge : les courses de taxi jaunes de New York (NYC TLC), mois de janvier
2024, fichier `yellow_tripdata_2024-01.parquet` (environ 3 millions de courses). On y ajoute la
table de correspondance des zones `taxi_zone_lookup.csv`, qui associe chaque `LocationID` à un
quartier (`Borough`) et à un nom de zone (`Zone`).

On suppose les deux fichiers présents dans `data/datasets/` :

```
data/datasets/yellow_tripdata_2024-01.parquet
data/datasets/taxi_zone_lookup.csv
```

Rappel du jour 3 : une transformation étroite (comme `filter` ou `withColumn`) traite chaque
partition indépendamment, sans déplacer de données. Une transformation large (comme `groupBy`,
`join` ou `distinct`) doit regrouper les données par clé à travers tout le cluster : c'est le
shuffle. Le shuffle écrit sur disque, transite par le réseau, et coupe le job en deux stages.
Notre but ici est de le voir de nos propres yeux.

## Consignes

Créez un fichier `tp07_spark_ui.py`. Démarrez une SparkSession en local et chargez les deux
fichiers. Un détail important : on demande explicitement à la Spark UI de rester accessible un
moment après la fin du job, sinon le serveur web s'éteint dès que le script se termine.

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP07 - Spark UI et explain")
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

# Affiche l'URL de la Spark UI (souvent http://localhost:4040)
print("Spark UI :", spark.sparkContext.uiWebUrl)
```

### 1. Construire un job avec un shuffle (groupBy puis join)

On veut le revenu total et le nombre de courses par zone de départ, avec le nom lisible de la
zone. Cela demande une agrégation (`groupBy` + `agg`) suivie d'une jointure (`join`). Les deux
sont des transformations larges, donc deux occasions de shuffle.

```python
stats_zone = (
    courses
    .groupBy("PULocationID")
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu_total"),
    )
)

resultat = (
    stats_zone
    .join(zones, stats_zone.PULocationID == zones.LocationID, "inner")
    .select("Borough", "Zone", "nb_courses", "revenu_total")
    .orderBy(F.desc("revenu_total"))
)
```

A ce stade, **rien n'a encore été calculé** : ce ne sont que des transformations. Notez-le, vous
le verrez confirmé dans la Spark UI à l'étape 4.

### 2. Lire le plan d'exécution avec explain(True)

Avant de déclencher le job, affichez le plan complet. `explain(True)` montre quatre plans : le
plan logique brut (`Parsed`), le plan logique analysé (`Analyzed`), le plan logique optimisé
(`Optimized`) et surtout le plan physique (`Physical Plan`), celui qui sera réellement exécuté.

```python
resultat.explain(True)
```

Dans le **plan physique** (la dernière section, en bas), repérez et notez :

- les lignes contenant `Exchange` : ce sont les shuffles. Combien y en a-t-il ?
- le mot-clé `hashpartitioning` à côté de chaque `Exchange` : sur quelle colonne Spark
  répartit-il les données ?
- les lignes `Scan parquet` et `Scan csv` (ou `FileScan`) : ce sont les lectures de fichiers.
  Regardez si `ReadSchema` ne contient que les colonnes utiles (`PULocationID`, `total_amount`,
  etc.) et non tout le fichier.

A compléter dans un commentaire de votre script : combien d'`Exchange` voyez-vous, et à quelle
opération de votre code Python chacun correspond-il (le `groupBy` ? le `join` ? le `orderBy` ?) ?

### 3. Déclencher le job avec une action

Le plan ne s'exécute que sur une action. Lancez-en une qui ramène peu de données au driver :

```python
resultat.show(10, truncate=False)
```

Notez le temps que cela prend (à l'oeil ou avec un chronomètre). On le comparera plus tard, au TP
d'optimisation, quand on remplacera la jointure par un broadcast join.

### 4. Ouvrir la Spark UI et lire le DAG

Le script ne doit pas se terminer tout de suite, sinon la Spark UI disparaît. Ajoutez une pause à
la fin pour avoir le temps d'explorer dans le navigateur :

```python
print("Ouvrez la Spark UI dans le navigateur, vous avez 120 secondes.")
print("URL :", spark.sparkContext.uiWebUrl)
time.sleep(120)

spark.stop()
```

Lancez le script (`python tp07_spark_ui.py`) puis ouvrez l'URL affichée (en général
`http://localhost:4040`). Explorez :

1. **Onglet Jobs** : repérez le job déclenché par `show`. Combien de stages a-t-il ? Cliquez
   dessus.
2. **DAG Visualization** (sur la page du job) : suivez les boîtes. Vous devez voir des frontières
   entre stages là où il y a un shuffle. Comptez les stages séparés par ces frontières.
3. **Onglet Stages** : repérez les colonnes `Shuffle Read` et `Shuffle Write`. Quel volume de
   données a transité par le shuffle ? (ordre de grandeur, en Mo)
4. **Onglet SQL / DataFrame** : cliquez sur la requête. Vous y retrouvez le même plan que
   `explain`, mais en version graphique. Repérez les boîtes `Exchange` : ce sont vos shuffles, les
   mêmes que ceux lus à l'étape 2.

### 5. Comparer un job sans shuffle

Pour bien sentir la différence, lancez un second traitement qui ne contient que des
transformations étroites (un `filter` puis un `select`) et déclenchez-le :

```python
courses_courtes = (
    courses
    .filter(F.col("trip_distance") < 2)
    .select("PULocationID", "trip_distance", "total_amount")
)
courses_courtes.explain(True)        # combien d'Exchange ici ?
print("Courses courtes :", courses_courtes.count())
```

Comparez le plan physique de ce traitement à celui de l'étape 2 : combien d'`Exchange` cette
fois ? Dans la Spark UI, ce job a-t-il un ou plusieurs stages ?

## Livrable

Vous avez réussi le TP si :

- Vous savez dire combien d'`Exchange` apparaissent dans le plan physique du traitement
  `groupBy` + `join` + `orderBy`, et à quelle ligne de votre code Python chacun correspond.
- Vous avez ouvert la Spark UI, trouvé le job déclenché par `show`, et vu qu'il est découpé en
  plusieurs stages, avec une frontière à chaque shuffle.
- Vous savez lire les colonnes `Shuffle Read` et `Shuffle Write` dans l'onglet Stages et en
  donner un ordre de grandeur.
- Vous constatez que le traitement de comparaison (`filter` + `select`) n'a **aucun** `Exchange`
  dans son plan et tient en un seul stage.
- Vous savez relier les trois vues d'un même shuffle : la transformation large dans le code,
  l'`Exchange` dans `explain`, et la frontière de stage dans le DAG.

## Aide

### Comment lire un plan physique

Le plan physique se lit de **bas en haut** : tout en bas, les `Scan` (lecture des fichiers) ;
tout en haut, l'opération finale (ici le tri). Entre les deux, chaque ligne est une étape. Les
opérations sont indentées : une ligne plus indentée se produit avant (plus tôt) la ligne moins
indentée au-dessus d'elle.

- `Exchange hashpartitioning(PULocationID, 200)` = un shuffle qui répartit les données par
  `PULocationID` en 200 partitions (200 est le nombre de partitions de shuffle par défaut,
  paramètre `spark.sql.shuffle.partitions`).
- `HashAggregate` = l'agrégation (le `groupBy`). Il apparaît souvent en deux fois : une fois
  avant le shuffle (agrégation partielle, par partition) et une fois après (agrégation finale).
- `SortMergeJoin` ou `BroadcastHashJoin` = la jointure. Un `SortMergeJoin` est précédé de deux
  `Exchange` (un par côté à joindre). Un `BroadcastHashJoin` n'a pas d'`Exchange` du tout : la
  petite table est diffusée. C'est la piste d'optimisation du TP suivant.
- `Sort` = le tri (le `orderBy`), lui aussi précédé d'un `Exchange rangepartitioning`.
- `*(1)`, `*(2)` au début des lignes : le numéro entre parenthèses est l'identifiant du stage
  (whole-stage codegen). Les opérations d'un même numéro sont dans le même stage ; le numéro
  change à chaque shuffle.

### Trouver et garder la Spark UI

- L'URL est imprimée par `spark.sparkContext.uiWebUrl`, en général `http://localhost:4040`. Si un
  autre job Spark tourne déjà, le port peut être `4041`, `4042`, etc.
- La Spark UI n'existe **que pendant que la SparkSession vit**. Si le script se termine, l'UI
  disparaît. D'où le `time.sleep(120)` avant `spark.stop()`. Autre option : lancer le code dans
  un shell interactif (`pyspark`) ou un notebook, qui gardent la session ouverte.
- Pour rejouer un job après coup, il existe le Spark History Server, mais il n'est pas nécessaire
  ici.

### Rappels d'API

- `df.explain()` affiche seulement le plan physique. `df.explain(True)` ajoute les plans logiques
  (parsed, analyzed, optimized). `df.explain("formatted")` donne une version plus lisible, avec
  les détails numérotés.
- `spark.sparkContext.uiWebUrl` renvoie l'URL de la Spark UI.
- `spark.conf.get("spark.sql.shuffle.partitions")` montre le nombre de partitions de shuffle (200
  par défaut). Vous le retrouverez dans les `Exchange`.
- Une action déclenche le calcul : `show`, `count`, `collect`, `write`. Une transformation
  (`groupBy`, `join`, `filter`, `select`, `orderBy`) ne déclenche rien toute seule.

### Commandes utiles

```bash
# Verifier que les deux fichiers sont la
ls -lh data/datasets/yellow_tripdata_2024-01.parquet data/datasets/taxi_zone_lookup.csv

# Lancer le script
python tp07_spark_ui.py

# Pendant la pause de 120 secondes, ouvrir la Spark UI
# macOS : open http://localhost:4040
# Linux : xdg-open http://localhost:4040
```

### Si quelque chose coince

- **L'URL `http://localhost:4040` ne répond pas** : soit le script s'est déjà terminé (la session
  est fermée), soit le port est pris et l'UI écoute sur `4041`. Relisez la ligne imprimée par
  `uiWebUrl`.
- **Je ne vois pas l'onglet SQL / DataFrame** : il n'apparaît qu'après au moins une action sur un
  DataFrame. Lancez le `show` d'abord, puis rafraîchissez la page.
- **Le nombre de stages me surprend** : c'est normal qu'il y en ait plus que de shuffles. Un job
  avec deux shuffles donne au moins trois stages (avant le premier shuffle, entre les deux, après
  le second). Comptez les frontières, pas seulement les `Exchange`.
- **`explain` montre un `BroadcastHashJoin` alors que je n'ai rien demandé** : Spark diffuse
  automatiquement les petites tables (la table des zones fait quelques Ko). C'est l'AQE et le
  seuil d'auto-broadcast. Pour forcer le `SortMergeJoin` à des fins pédagogiques, on peut couper
  l'auto-broadcast. C'est justement le sujet du TP d'optimisation.
