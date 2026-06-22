# Pipeline d'ingestion : du CSV open data au Parquet partitionné

## Objectif

Construire un mini pipeline d'ingestion de bout en bout, comme en fait un data engineer au
quotidien. Vous allez :

1. lire un CSV d'open data avec un **schéma explicite** (`StructType`), pas par inférence ;
2. **nettoyer** les données : valeurs manquantes (`na`), doublons (`dropDuplicates`), valeurs
   aberrantes (filtrage) ;
3. **écrire** le résultat en **Parquet partitionné** (`write.partitionBy(...)`) ;
4. **relire** le Parquet et **vérifier** que tout est cohérent (schéma, comptes, partitions sur
   disque).

À la fin de ce TP, vous savez transformer un fichier brut et bancal en une table Parquet propre,
typée et bien rangée, prête à être interrogée efficacement.

## Contexte

Le fil rouge du cours, ce sont les courses de taxi de New York, déjà au format Parquet : tout y
est propre et typé. C'est confortable, mais ce n'est pas le cas le plus fréquent dans la vraie vie.
La plupart des données arrivent en **CSV brut** : pas de schéma, des colonnes à moitié vides, des
doublons, des valeurs absurdes. Le travail d'ingestion consiste justement à fiabiliser ces données
avant de les exploiter.

On change donc de jeu pour ce TP et on prend de l'**open data français** : les **DVF** (Demandes de
valeurs foncières), c'est-à-dire toutes les transactions immobilières déclarées en France. Le
fichier fourni couvre **Paris (département 75) pour l'année 2023**, au format CSV compressé gzip :

```
data/datasets/dvf_75_2023.csv.gz
```

Spark lit nativement le gzip, il n'y a rien à décompresser. Si vous préférez, ce TP fonctionne à
l'identique avec les **accidents corporels de la route (ONISR)**, fichier `caracteristiques` :
les consignes sont les mêmes, seuls le nom des colonnes et le séparateur (`;` au lieu de `,`)
changent. Le reste de l'énoncé suppose le fichier DVF.

Colonnes utiles du fichier DVF (il en contient beaucoup d'autres, qu'on ignorera) :

- `date_mutation` : date de la transaction (texte `AAAA-MM-JJ`).
- `valeur_fonciere` : prix de la transaction en euros (texte à convertir, parfois vide).
- `code_postal` : code postal (texte, sur 5 caractères, parfois vide).
- `nom_commune` : nom de la commune.
- `code_departement` : code du département (ici toujours `75`).
- `type_local` : nature du bien (`Appartement`, `Maison`, `Local industriel. commercial...`,
  `Dependance`, ou vide).
- `surface_reelle_bati` : surface bâtie en mètres carrés (parfois vide ou nulle).
- `nombre_pieces_principales` : nombre de pièces.
- `longitude`, `latitude` : coordonnées géographiques.

## Consignes

Créez un fichier `tp05_ingestion.py`. Démarrez une SparkSession en local :

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType,
)

spark = (
    SparkSession.builder
    .appName("TP05 - Ingestion Parquet")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SOURCE = "data/datasets/dvf_75_2023.csv.gz"
CIBLE = "data/datasets/dvf_75_parquet"   # dossier de sortie Parquet
```

### 1. Définir un schéma explicite

Plutôt que de laisser Spark deviner les types (`inferSchema`, qui lit tout le fichier deux fois et
se trompe parfois), on **déclare** les colonnes qu'on veut garder et leur type. Complétez le
`StructType` ci-dessous. On ne déclare que les colonnes utiles : les autres colonnes du CSV seront
simplement ignorées à la lecture (Spark ne garde que les noms présents dans le schéma).

```python
schema = StructType([
    StructField("date_mutation", DateType(), True),
    StructField("valeur_fonciere", DoubleType(), True),
    StructField("code_postal", StringType(), True),
    StructField("nom_commune", StringType(), True),
    StructField("code_departement", StringType(), True),
    StructField("type_local", StringType(), True),
    StructField("surface_reelle_bati", DoubleType(), True),
    StructField("nombre_pieces_principales", ________, True),   # un entier
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
])
```

> Le booléen final de chaque `StructField` indique si la colonne est `nullable` (autorise les
> valeurs nulles). Ici on met `True` partout : les données brutes sont pleines de trous.

### 2. Lire le CSV avec ce schéma

Lisez le CSV en passant le schéma explicite. Comme le fichier a un en-tête, mais que les noms du
schéma doivent correspondre, on garde `header=True`. On force aussi le format de date :

```python
brut = (
    spark.read
    .option("header", True)
    .option("sep", ",")              # virgule pour DVF ; ";" pour ONISR
    .option("dateFormat", "yyyy-MM-dd")
    .schema(________)                # passer le schema defini a l'etape 1
    .csv(SOURCE)
)

brut.printSchema()
print("Lignes brutes :", brut.count())
brut.show(5, truncate=False)
```

Vérifiez dans `printSchema()` que `valeur_fonciere` est bien un `double` et `date_mutation` une
`date`, et non des `string`.

### 3. Nettoyer : valeurs manquantes (na)

Une transaction sans prix ou sans surface ne sert à rien pour une analyse de prix au mètre carré.
On retire donc les lignes où les colonnes essentielles sont nulles, avec `na.drop`.

```python
essentielles = ["valeur_fonciere", "surface_reelle_bati", "type_local"]

sans_na = brut.na.drop(subset=________)   # ne garder que les lignes ou ces 3 colonnes sont non nulles

print("Apres suppression des na :", sans_na.count())
```

### 4. Nettoyer : doublons

Le fichier DVF contient des doublons (une même mutation peut apparaître sur plusieurs lignes à
cause des lots et parcelles). Supprimez les doublons stricts :

```python
sans_doublons = sans_na.________()        # methode qui supprime les lignes entierement identiques

print("Apres suppression des doublons :", sans_doublons.count())
```

### 5. Nettoyer : valeurs aberrantes

Même typées et non nulles, certaines valeurs sont absurdes et fausseraient les statistiques :
prix nul ou négatif, surface nulle, prix au mètre carré délirant (un studio à 50 euros, ou un bien
à 2 millions d'euros le mètre carré). On filtre. On en profite pour calculer le **prix au mètre
carré**, qui sera utile pour l'analyse.

```python
propre = (
    sans_doublons
    .filter(
        (F.col("valeur_fonciere") > 0) &
        (F.col("surface_reelle_bati") > 0) &
        (F.col("type_local").isin("Appartement", "Maison"))
    )
    .withColumn("prix_m2", F.round(F.col("valeur_fonciere") / F.col("surface_reelle_bati"), 0))
    # A completer : ne garder que les prix au m2 plausibles (entre 1000 et 50000 euros/m2 a Paris)
    .filter((F.col("prix_m2") >= ________) & (F.col("prix_m2") <= ________))
    .withColumn("mois", F.month("date_mutation"))   # colonne de partitionnement
)

print("Apres nettoyage des aberrations :", propre.count())
propre.select("nom_commune", "type_local", "surface_reelle_bati", "valeur_fonciere", "prix_m2").show(5)
```

### 6. Écrire en Parquet partitionné

Écrivez le DataFrame propre en Parquet, **partitionné par mois** (`mois`). Le partitionnement crée
un sous-dossier par valeur, ce qui permettra plus tard de ne lire que les mois utiles (partition
pruning). Utilisez le mode `overwrite` pour pouvoir relancer le script sans erreur :

```python
(
    propre
    .write
    .mode("________")                 # ecraser la sortie si elle existe deja
    .partitionBy("________")          # un sous-dossier par mois
    .parquet(CIBLE)
)
print("Ecriture terminee dans", CIBLE)
```

### 7. Relire et vérifier

C'est l'étape de contrôle. Relisez le Parquet écrit (sans schéma cette fois : le Parquet embarque
le sien) et vérifiez que tout colle :

```python
relu = spark.read.parquet(CIBLE)

relu.printSchema()                        # le schema est-il bien type ?
print("Lignes relues :", relu.count())    # doit egaler le compte de l'etape 5

# La colonne de partitionnement est-elle bien la, reconstruite depuis les dossiers ?
relu.groupBy("mois").count().orderBy("mois").show()

# Verification metier : prix au m2 moyen par type de bien
relu.groupBy("type_local").agg(
    F.round(F.avg("prix_m2"), 0).alias("prix_m2_moyen"),
    F.count("*").alias("nb_ventes"),
).show()
```

Inspectez aussi le dossier de sortie sur disque pour voir le partitionnement :

```bash
ls data/datasets/dvf_75_parquet/
# attendu : des sous-dossiers mois=1/, mois=2/, ... mois=12/ et un fichier _SUCCESS
```

N'oubliez pas `spark.stop()` à la fin.

## Livrable

Vous avez réussi le TP si :

- Le schéma est **déclaré explicitement** (pas d'`inferSchema`), et `printSchema()` montre bien
  `valeur_fonciere: double`, `date_mutation: date`, `nombre_pieces_principales: integer`.
- Les trois étapes de nettoyage réduisent le nombre de lignes à chaque fois (na, puis doublons,
  puis aberrations), et vous savez dire combien de lignes ont sauté à chaque étape.
- La colonne `prix_m2` existe et ne contient pas de valeurs absurdes (ni nulles, ni des millions).
- Le dossier `data/datasets/dvf_75_parquet/` existe et contient des sous-dossiers `mois=.../`
  ainsi qu'un fichier `_SUCCESS`.
- La relecture donne **exactement le même nombre de lignes** que le DataFrame propre écrit, et la
  colonne `mois` est bien présente après relecture (Spark la reconstruit depuis les noms de
  dossiers).
- Le prix au mètre carré moyen pour les appartements parisiens tombe dans un ordre de grandeur
  réaliste (autour de 10 000 euros/m2), ce qui confirme que le nettoyage a bien fait son travail.

## Aide

### Pourquoi un schéma explicite ?

- `inferSchema=True` oblige Spark à lire le fichier une première fois juste pour deviner les types,
  puis une seconde fois pour les données. Sur un gros CSV, c'est deux fois plus lent.
- L'inférence se trompe : un code postal comme `01000` devient l'entier `1000` (le zéro saute), une
  colonne presque vide est typée `string` au hasard, une date reste du texte.
- Un schéma explicite documente le contrat de données et échoue tôt si le fichier change.

### Rappels d'API

- Types à importer depuis `pyspark.sql.types` : `StructType`, `StructField`, `StringType`,
  `IntegerType`, `DoubleType`, `DateType`, `TimestampType`, `BooleanType`.
- Un `StructField` se construit ainsi : `StructField("nom_colonne", TypeType(), True)` où le
  dernier argument est `nullable`.
- Lecture CSV avec schéma : `spark.read.option("header", True).schema(mon_schema).csv(chemin)`.
- Valeurs manquantes :
  - `df.na.drop()` supprime les lignes contenant au moins un null.
  - `df.na.drop(subset=["a", "b"])` ne regarde que ces colonnes.
  - `df.na.fill(0, subset=["a"])` remplace les null par une valeur.
- Doublons :
  - `df.dropDuplicates()` supprime les lignes entièrement identiques.
  - `df.dropDuplicates(["id"])` ne garde qu'une ligne par valeur de `id`.
- `F.col("type_local").isin("Appartement", "Maison")` garde uniquement ces deux valeurs.
- `F.month("date_mutation")` extrait le numéro du mois (1 à 12) d'une colonne de type date.
- Écriture : `df.write.mode("overwrite").partitionBy("mois").parquet(chemin)`.
  Modes : `overwrite`, `append`, `error` (par défaut), `ignore`.

### Commandes utiles

```bash
# Verifier que le fichier source est la
ls -lh data/datasets/dvf_75_2023.csv.gz

# Regarder l'en-tete du CSV (gzip) pour reperer les vrais noms de colonnes
gunzip -c data/datasets/dvf_75_2023.csv.gz | head -1 | tr ',' '\n' | nl

# Apres ecriture, voir l'arborescence partitionnee
ls -R data/datasets/dvf_75_parquet/ | head -30
```

### Si quelque chose coince

- **`Path does not exist`** : le fichier DVF n'est pas téléchargé. Lancez `bash data/download.sh`
  (il récupère `dvf_75_2023.csv.gz`).
- **Beaucoup de `null` après lecture, ou tout en `null`** : le séparateur ou les noms de colonnes
  ne correspondent pas. Vérifiez l'en-tête réel du CSV (commande `gunzip ... | head -1` ci-dessus)
  et que `header=True`. Pour ONISR, le séparateur est `;`.
- **`valeur_fonciere` toujours nulle alors qu'il y a des valeurs** : dans certains exports le
  séparateur décimal est la virgule, ce qui entre en conflit avec le séparateur de colonnes.
  Le fichier geo-dvf d'Etalab utilise le point comme séparateur décimal et la virgule pour les
  colonnes : c'est le cas standard, mais vérifiez sur les premières lignes.
- **L'écriture échoue avec `path already exists`** : vous n'avez pas mis `mode("overwrite")`.
- **`relu.count()` diffère du compte écrit** : vous avez probablement relu un autre chemin, ou une
  écriture précédente a laissé des fichiers. Supprimez le dossier de sortie et relancez.
