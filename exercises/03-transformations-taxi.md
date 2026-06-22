# Transformer et enrichir les courses de taxi

## Objectif

Passer du DataFrame brut à un DataFrame enrichi qui répond à des questions métier. Vous allez
créer des colonnes dérivées avec `withColumn` (durée de course en minutes, prix au kilomètre),
classer les courses avec `when` / `otherwise`, agréger avec `groupBy` / `agg` (revenu et
pourboire moyen par zone), puis joindre le résultat avec la table des zones pour remplacer les
identifiants numériques par des noms lisibles (quartier et zone de New York).

À la fin de ce TP, vous savez transformer, agréger et joindre des DataFrames Spark, ce qui est le
coeur du travail quotidien d'un data engineer.

## Contexte

On reste sur le fil rouge : les courses de taxi jaunes de New York (NYC TLC), mois de janvier
2024, fichier `yellow_tripdata_2024-01.parquet`. Ce fichier contient environ 3 millions de
courses, avec entre autres les colonnes :

- `tpep_pickup_datetime` et `tpep_dropoff_datetime` : horodatages de prise en charge et de
  dépose (type timestamp).
- `trip_distance` : distance de la course en miles.
- `PULocationID` et `DOLocationID` : identifiants numériques des zones de départ et d'arrivée.
- `fare_amount`, `tip_amount`, `total_amount` : montant de la course, pourboire, total (dollars).
- `payment_type` : mode de paiement (entier ; 1 = carte de crédit, 2 = espèces, etc.).

Les identifiants de zone ne parlent à personne tels quels. C'est là qu'intervient la table de
correspondance `taxi_zone_lookup.csv`, qui associe chaque `LocationID` à un quartier (`Borough`)
et à un nom de zone (`Zone`). On la joindra à la fin pour produire un résultat lisible.

On suppose les deux fichiers présents dans `data/datasets/` :

```
data/datasets/yellow_tripdata_2024-01.parquet
data/datasets/taxi_zone_lookup.csv
```

## Consignes

Créez un fichier `tp03_transformations.py`. Démarrez une SparkSession en local et chargez le
Parquet, comme au TP précédent :

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP03 - Transformations taxi")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
```

### 1. Créer la durée de course en minutes

La durée d'une course n'est pas une colonne du fichier : il faut la calculer à partir des deux
horodatages. Complétez le code pour ajouter une colonne `duree_min` (durée en minutes, arrondie) :

```python
courses = courses.withColumn(
    "duree_min",
    (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60
)

# A completer : arrondir duree_min a 1 decimale avec F.round(...)
courses = courses.withColumn("duree_min", F.round(F.col("duree_min"), ____))
```

Vérifiez avec `courses.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "duree_min").show(5)`.

### 2. Créer le prix au kilomètre

Le fichier donne `trip_distance` en miles. On veut un prix au kilomètre, plus parlant en France.
Convertissez d'abord la distance en kilomètres (1 mile = 1,60934 km), puis calculez le prix au km
à partir de `fare_amount`. Attention à la division par zéro quand la distance est nulle.

```python
courses = courses.withColumn("distance_km", F.col("trip_distance") * 1.60934)

# A completer : prix_par_km = fare_amount / distance_km, mais seulement si distance_km > 0
courses = courses.withColumn(
    "prix_par_km",
    F.when(F.col("distance_km") > 0, F.round(F.col("fare_amount") / F.col("distance_km"), 2))
     .otherwise(____)   # valeur a renvoyer quand la distance est nulle (None par exemple)
)
```

### 3. Classer les courses par distance avec when / otherwise

Ajoutez une colonne catégorielle `categorie` qui range chaque course selon sa distance en km :

- moins de 2 km : `"courte"`
- de 2 km (inclus) à 8 km (exclu) : `"moyenne"`
- 8 km et plus : `"longue"`

```python
courses = courses.withColumn(
    "categorie",
    F.when(F.col("distance_km") < 2, "courte")
     .when(F.col("distance_km") < 8, "moyenne")
     .otherwise(____)    # le reste : "longue"
)
```

Affichez la répartition : `courses.groupBy("categorie").count().show()`.

### 4. Nettoyer les valeurs aberrantes

Avant d'agréger, on filtre les courses incohérentes (elles fausseraient les moyennes) : durée
nulle ou négative, durée absurde (plus de 3 heures), distance nulle, montant négatif.

```python
courses_propres = courses.filter(
    (F.col("duree_min") > 0) &
    (F.col("duree_min") < 180) &
    (F.col("distance_km") > 0) &
    (F.col("fare_amount") >= 0)
)
```

Comparez `courses.count()` et `courses_propres.count()` : combien de lignes ont été écartées ?

### 5. Agréger : revenu et pourboire moyen par zone de départ

Groupez par zone de départ (`PULocationID`) et calculez, en une seule passe :

- le nombre de courses,
- le revenu total (`F.sum` sur `total_amount`),
- le pourboire moyen (`F.avg` sur `tip_amount`),
- la durée moyenne.

```python
stats_zone = (
    courses_propres
    .groupBy("PULocationID")
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu_total"),
        F.round(F.avg("tip_amount"), 2).alias("pourboire_moyen"),
        # A completer : duree moyenne arrondie, alias "duree_moyenne"
        ____
    )
)
```

### 6. Joindre avec la table des zones

Les `PULocationID` restent des nombres. Chargez `taxi_zone_lookup.csv` et joignez pour obtenir le
quartier (`Borough`) et le nom de zone (`Zone`). La clé côté lookup est `LocationID`.

```python
zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
)
# Colonnes : LocationID, Borough, Zone, service_zone

resultat = (
    stats_zone
    .join(zones, stats_zone.PULocationID == zones.LocationID, "left")
    .select("Borough", "Zone", "nb_courses", "revenu_total", "pourboire_moyen", "duree_moyenne")
)
```

### 7. Répondre aux questions métier

Affichez :

1. Le top 10 des zones par revenu total (tri décroissant).
2. Le top 10 des zones par pourboire moyen, en ne gardant que les zones avec au moins
   1000 courses (sinon une zone minuscule avec un seul gros pourboire remonte en tête).

```python
# Top 10 revenu
resultat.orderBy(F.desc("revenu_total")).show(10, truncate=False)

# Top 10 pourboire moyen, zones significatives uniquement
(resultat
 .filter(F.col("nb_courses") >= 1000)
 .orderBy(F.desc("pourboire_moyen"))
 .show(10, truncate=False))
```

N'oubliez pas `spark.stop()` à la fin.

## Livrable

Vous avez réussi le TP si :

- `courses` possède les colonnes `duree_min`, `distance_km`, `prix_par_km` et `categorie`, avec
  des valeurs cohérentes (une course de 5 km dure quelques minutes, pas plusieurs heures).
- `prix_par_km` ne plante pas sur les courses de distance nulle (valeur nulle, pas une erreur de
  division par zéro).
- `groupBy("categorie").count()` montre les trois catégories `courte`, `moyenne`, `longue`.
- Le filtrage des valeurs aberrantes écarte un nombre raisonnable de lignes (de l'ordre de
  quelques pour cent, pas la moitié du fichier).
- `stats_zone` contient bien `nb_courses`, `revenu_total`, `pourboire_moyen`, `duree_moyenne`.
- Après la jointure, le résultat affiche des noms lisibles (`Borough`, `Zone`) et non des
  identifiants numériques.
- Le top 10 par revenu fait remonter des zones de Manhattan (aéroports et centre), ce qui est
  cohérent avec la réalité des taxis new-yorkais.

## Aide

### Rappels d'API

- `withColumn("nom", expression)` ajoute (ou remplace si le nom existe) une colonne. L'expression
  est un objet `Column`, construit avec `F.col(...)` et les opérateurs Python (`+`, `-`, `*`, `/`,
  `>`, `<`, `&`, `|`).
- Différence de timestamps : `cast("long")` convertit un timestamp en nombre de secondes depuis
  1970. La soustraction donne donc des secondes ; diviser par 60 donne des minutes.
- `F.when(condition, valeur).when(...).otherwise(valeur)` est le `CASE WHEN` de SQL. On peut
  chaîner plusieurs `.when(...)`. La première condition vraie l'emporte, d'où l'ordre des seuils
  dans la catégorisation.
- `F.round(colonne, n)` arrondit à `n` décimales.
- Combiner des conditions dans un `filter` : entourer chaque condition de parenthèses et utiliser
  `&` (et), `|` (ou), `~` (non). Le mot-clé Python `and` ne fonctionne pas sur les colonnes Spark.
- `groupBy(...).agg(...)` : à l'intérieur de `agg`, utiliser `F.count`, `F.sum`, `F.avg`, `F.min`,
  `F.max`. Penser à `.alias("nom")` pour nommer chaque colonne résultat.
- `F.count("*")` compte les lignes du groupe ; `F.countDistinct("col")` compte les valeurs
  distinctes.
- Jointure : `gauche.join(droite, condition, "left")`. Types utiles : `"inner"` (par défaut),
  `"left"`, `"right"`, `"outer"`. Ici `"left"` garde toutes les zones de départ même si un
  identifiant manque dans la table de correspondance.

### Commandes utiles

```bash
# Verifier que les deux fichiers sont la
ls -lh data/datasets/yellow_tripdata_2024-01.parquet data/datasets/taxi_zone_lookup.csv

# Jeter un oeil aux premieres lignes du CSV des zones
head -5 data/datasets/taxi_zone_lookup.csv
```

### Si quelque chose coince

- **`AnalysisException: cannot resolve 'duree_min'`** : la colonne n'existe pas encore au moment
  où vous l'utilisez. Vérifiez que les `withColumn` sont exécutés dans l'ordre (chaque étape
  réaffecte `courses`).
- **Une erreur de division ou des `Infinity` dans `prix_par_km`** : c'est la division par zéro sur
  les distances nulles. Le `F.when(distance_km > 0, ...).otherwise(None)` est justement là pour
  l'éviter ; vérifiez que la condition est bien écrite.
- **Le top 10 par pourboire fait remonter des zones inconnues avec très peu de courses** : c'est
  attendu sans le filtre `nb_courses >= 1000`. Les espèces ne portent pas de pourboire dans ces
  données, donc le pourboire moyen est surtout piloté par les paiements par carte.
- **Après la jointure, des colonnes en double ou ambiguës** : on a fait un `.select(...)` juste
  après le `join` pour ne garder que les colonnes utiles et éviter l'ambiguïté entre
  `PULocationID` et `LocationID`.
