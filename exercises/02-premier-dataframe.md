# Premier DataFrame : charger et explorer les courses taxi

## Objectif

Charger le fichier Parquet des taxis de New York dans un DataFrame, inspecter son schéma avec
`printSchema()`, sélectionner et filtrer des colonnes, utiliser `describe()` pour un premier
diagnostic statistique, puis répondre à trois questions métier simples. À la fin de ce TP, vous
savez ouvrir un jeu de données réel et l'interroger avec l'API DataFrame de Spark.

## Contexte

Notre fil rouge est le jeu de données des courses de taxi jaunes de New York (NYC TLC). Au jour 1,
vous avez déjà installé PySpark et lu le fichier `yellow_tripdata_2024-01.parquet` avec un simple
`show()` et `count()`. On passe maintenant à l'API DataFrame, le bon niveau d'abstraction pour
travailler au quotidien (schéma, colonnes typées, optimiseur Catalyst).

On suppose les fichiers présents dans `data/datasets/` :

- `yellow_tripdata_2024-01.parquet` : environ 3 millions de courses du mois de janvier 2024.
- Colonnes utiles ici : `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`,
  `trip_distance` (distance en miles), `PULocationID`, `DOLocationID`, `fare_amount`,
  `tip_amount`, `total_amount`, `payment_type`.

Le Parquet embarque son propre schéma : pas besoin d'inférer les types ni de déclarer un
`StructType`, contrairement à un CSV. C'est tout l'intérêt du format pour démarrer vite.

## Consignes

### 1. Créer la session et charger le DataFrame

Créez un fichier `tp02_premier_dataframe.py`. Complétez le squelette ci-dessous pour créer une
SparkSession en mode local et lire le Parquet :

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP02 - Premier DataFrame")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

chemin = "data/datasets/yellow_tripdata_2024-01.parquet"

# A completer : lire le fichier Parquet dans un DataFrame nomme df
df = spark.read.________(chemin)

print("Type de df :", type(df))
```

Lancez le script avec `python tp02_premier_dataframe.py` et vérifiez qu'aucune erreur n'apparaît.

### 2. Inspecter le schéma et un échantillon

Avant toute transformation, on regarde la structure des données. Complétez :

```python
# A completer : afficher le schema (nom et type de chaque colonne)
df.________()

# A completer : afficher 5 lignes sans tronquer les colonnes
df.show(5, truncate=________)

# A completer : lister juste les noms de colonnes (attribut Python, pas une methode)
print("Colonnes :", df.________)
```

Repérez bien les colonnes citées dans le contexte. Notez les types : `trip_distance` doit être un
`double`, les dates des `timestamp`, `PULocationID` un entier.

### 3. Sélectionner des colonnes

On réduit le DataFrame aux colonnes qui nous intéressent. Complétez avec `select` :

```python
# A completer : ne garder que ces 5 colonnes
colonnes = ["tpep_pickup_datetime", "trip_distance", "PULocationID", "tip_amount", "total_amount"]
df_court = df.________(colonnes)

df_court.show(5, truncate=False)
```

### 4. Filtrer les courses

On veut isoler les courses qui ont au moins un passager et une distance strictement positive.
Complétez le filtre (deux conditions reliées par un `&`, chaque condition entre parenthèses) :

```python
df_valides = df.________(
    (F.col("passenger_count") > 0) ________ (F.col("trip_distance") > 0)
)

print("Courses valides :", df_valides.________())
```

### 5. Statistiques rapides avec describe

`describe()` donne d'un coup count, moyenne, écart-type, min et max sur des colonnes numériques :

```python
# A completer : statistiques sur ces trois colonnes
df.describe("trip_distance", "fare_amount", "total_amount").show()
```

Regardez les valeurs `min` et `max`. Vous verrez probablement des aberrations (distance ou montant
négatif, valeurs énormes). C'est normal sur des données brutes, on les traitera au jour 2.

### 6. Répondre à trois questions

En vous appuyant sur le DataFrame filtré `df_valides`, répondez aux trois questions suivantes.
Chaque réponse tient en une à deux lignes de code.

**Question A.** Combien y a-t-il de courses valides au total ?

```python
nb_courses = df_valides.________()
print("A. Nombre de courses valides :", nb_courses)
```

**Question B.** Quelle est la distance moyenne d'une course (en miles) ?

```python
# Indice : groupBy() sans argument + agg, ou directement select + agg
df_valides.________(F.avg("trip_distance").alias("distance_moyenne")).show()
```

**Question C.** Quelle est la course la plus longue en distance, et combien a-t-elle coûté ?

```python
df_valides.________(
    F.max("trip_distance").alias("distance_max"),
    F.max("total_amount").alias("montant_max")
).show()
```

Pensez à fermer la session à la fin du script avec `spark.stop()`.

## Livrable

Vous avez réussi le TP si :

- Le script s'exécute de bout en bout sans erreur.
- `df.printSchema()` affiche bien les colonnes attendues avec leurs types (`trip_distance` en
  `double`, les dates en `timestamp`).
- `df.columns` liste tous les noms de colonnes du fichier.
- Le `select` produit un DataFrame à 5 colonnes seulement.
- Le filtre `passenger_count > 0` et `trip_distance > 0` réduit le nombre de lignes (le compte des
  courses valides est inférieur au `count()` total).
- `describe()` affiche count, mean, stddev, min et max, et vous savez pointer au moins une valeur
  aberrante.
- Vous avez une réponse chiffrée pour les trois questions A, B et C, cohérente avec les ordres de
  grandeur attendus (voir l'aide).

## Aide

### Rappels d'API

- Lecture Parquet : `spark.read.parquet(chemin)`. Le schéma est lu depuis le fichier.
- Schéma : `df.printSchema()` (méthode). Liste des colonnes : `df.columns` (attribut, sans
  parenthèses).
- Affichage : `df.show(5, truncate=False)`. Avec `truncate=True` (défaut), les valeurs longues sont
  coupées à 20 caractères.
- Sélection : `df.select("col1", "col2")` ou `df.select(["col1", "col2"])` ou
  `df.select(F.col("col1"))`.
- Filtre : `df.filter(condition)` (ou son alias `df.where(condition)`). Pour combiner des
  conditions, utiliser `&` (et), `|` (ou), `~` (non), avec **chaque condition entre parenthèses** :
  `df.filter((F.col("a") > 0) & (F.col("b") > 0))`.
- Référence à une colonne : `F.col("nom_colonne")`.
- Statistiques : `df.describe("col1", "col2").show()`. Variante plus riche : `df.summary()`.
- Agrégation globale (sans groupe) : `df.agg(F.avg("col").alias("moyenne")).show()` ou
  `df.groupBy().agg(...)`. Fonctions utiles : `F.count`, `F.avg`, `F.sum`, `F.min`, `F.max`.
- `alias("nom")` renomme la colonne résultat pour un affichage lisible.

### Commandes utiles

```bash
# Verifier que le fichier de donnees est bien la
ls -lh data/datasets/yellow_tripdata_2024-01.parquet

# Lancer le script depuis la racine du projet (pour que data/datasets/... soit valide)
python tp02_premier_dataframe.py
```

### Ordres de grandeur attendus

- Nombre total de courses (janvier 2024) : environ 3 millions.
- Nombre de courses valides après filtre : un peu moins, de l'ordre de 2,9 millions.
- Distance moyenne d'une course : environ 3 à 4 miles.
- Vous verrez des valeurs `min` négatives sur `fare_amount` ou `total_amount` (remboursements,
  saisies erronées) et des `max` très élevés : ce sont des aberrations à garder en tête pour le
  nettoyage du jour 2.

### Si quelque chose coince

- **`Path does not exist`** : lancez le script depuis la racine du projet, ou utilisez un chemin
  absolu vers `data/datasets/`.
- **`Column is not iterable` ou erreur sur le filtre** : oubli des parenthèses autour de chaque
  condition. Écrire `(F.col("a") > 0) & (F.col("b") > 0)`, pas
  `F.col("a") > 0 & F.col("b") > 0`.
- **`AttributeError` sur `df.columns()`** : `columns` est un attribut, il s'écrit sans
  parenthèses : `df.columns`.
- **Le `count()` est lent** : c'est normal, c'est une action qui lit tout le fichier. Les
  `printSchema()` et `select()` qui précèdent sont paresseux et instantanés.
