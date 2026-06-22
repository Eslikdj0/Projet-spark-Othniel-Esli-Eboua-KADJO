# Mise en route de PySpark

## Objectif

Installer PySpark, vérifier la chaîne Java plus Python, lancer une première SparkSession en
local, lire le fichier Parquet des taxis de New York, et exécuter un premier `show()` et un
premier `count()`. À la fin de ce TP, votre environnement est prêt pour les trois jours de cours.

## Contexte

Notre fil rouge est le jeu de données des courses de taxi jaunes de New York (NYC TLC). Ces
données sont publiées au format Parquet, un format colonnaire compressé, avec environ
3 millions de lignes par mois. Nous travaillons avec le mois de janvier 2024,
`yellow_tripdata_2024-01.parquet`, accompagné de la table des zones `taxi_zone_lookup.csv`
(utile plus tard pour les jointures). On suppose les fichiers présents dans `data/datasets/`.

Avant de manipuler quoi que ce soit, il faut un environnement qui tourne : Spark s'appuie sur la
machine virtuelle Java (JVM), et PySpark est la passerelle Python vers cette JVM. On vérifie donc
d'abord Java et Python, puis on installe PySpark, puis on démarre une session.

## Consignes

### 1. Vérifier Java et Python

Spark 4 tourne sur la JVM. Il faut un Java 17 ou 21 (Spark 4 a abandonné les JDK 8 et 11), et un
Python 3.9 ou supérieur. Vérifiez les deux dans un terminal :

```bash
java -version
# attendu : openjdk version "17.x" ou "21.x"

python3 --version
# attendu : Python 3.9 ou superieur
```

Si `java -version` échoue ou affiche une version non supportée, installez un JDK 17 (par exemple
Temurin via Adoptium, ou `brew install openjdk@17` sur macOS, ou `apt install openjdk-17-jdk` sur
Debian / Ubuntu). Notez la valeur de `JAVA_HOME` si Spark ne trouve pas Java automatiquement.

### 2. Installer PySpark dans un environnement isolé

On installe PySpark dans un environnement virtuel pour ne pas polluer le Python système :

```bash
# Creer et activer un environnement virtuel
python3 -m venv .venv
source .venv/bin/activate     # sous Windows : .venv\Scripts\activate

# Installer PySpark (la version embarque sa propre distribution Spark)
pip install pyspark

# Verifier
pyspark --version
# attendu : la banniere Spark avec "version 4.x"
```

### 3. Lancer une première SparkSession en local

Créez un fichier `tp00_environnement.py`. Complétez le code ci-dessous pour créer une session
Spark en mode local (utiliser tous les coeurs disponibles avec `local[*]`) :

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP00 - Environnement")
    .master("local[*]")            # mode local, tous les coeurs
    .getOrCreate()
)

# Reduire le bruit dans la console
spark.sparkContext.setLogLevel("WARN")

print("Version de Spark :", spark.version)
print("Master :", spark.sparkContext.master)
```

Lancez le script :

```bash
python tp00_environnement.py
```

### 4. Lire le Parquet des taxis et l'explorer

Complétez le script pour lire le fichier Parquet, afficher quelques lignes et compter le nombre
total de courses. Le chemin pointe vers `data/datasets/` :

```python
chemin = "data/datasets/yellow_tripdata_2024-01.parquet"

df = spark.read.parquet(chemin)

# A completer : afficher 5 lignes sans tronquer les colonnes
df.show(...)

# A completer : afficher le schema (types de chaque colonne)
df.________()

# A completer : compter le nombre de lignes
nb = df.________()
print("Nombre de courses :", nb)
```

### 5. Garder la Spark UI ouverte, puis fermer proprement

Tant que la session vit, une interface web (la Spark UI) est disponible sur
`http://localhost:4040`. Ajoutez une pause avant la fin du script pour aller la regarder, puis
fermez la session :

```python
input("Spark UI sur http://localhost:4040 - appuyez sur Entree pour quitter...")

spark.stop()
```

Ouvrez `http://localhost:4040` dans un navigateur pendant la pause et repérez l'onglet **Jobs**.

## Livrable

Vous avez réussi le TP si :

- `java -version` et `python3 --version` renvoient des versions supportées (Java 17 ou 21,
  Python 3.9 ou supérieur).
- `pyspark --version` affiche bien Spark 4.x.
- Le script s'exécute sans erreur et affiche la version de Spark et le master `local[*]`.
- `df.show(5, truncate=False)` affiche 5 courses avec toutes les colonnes lisibles.
- `df.printSchema()` montre les colonnes attendues (`tpep_pickup_datetime`, `trip_distance`,
  `PULocationID`, `total_amount`, etc.).
- `df.count()` renvoie un nombre de l'ordre de quelques millions de lignes pour un mois.
- Vous avez ouvert la Spark UI sur `http://localhost:4040` et vu au moins un job.

## Aide

### Rappels d'API

- Création de session : `SparkSession.builder.appName("...").master("local[*]").getOrCreate()`.
- Lecture Parquet : `spark.read.parquet("chemin/vers/fichier.parquet")`. Le schéma est lu
  automatiquement depuis le fichier, pas besoin de `inferSchema`.
- Afficher des lignes : `df.show(5, truncate=False)`. Sans `truncate=False`, les longues valeurs
  sont coupées à 20 caractères.
- Afficher le schéma : `df.printSchema()`.
- Compter : `df.count()` renvoie un entier Python. C'est une **action** : elle déclenche un vrai
  calcul (contrairement à `df.read.parquet(...)` qui est paresseux).

### Commandes utiles

```bash
# Verifier ou est installe Java (macOS)
/usr/libexec/java_home -V

# Definir JAVA_HOME si Spark ne trouve pas Java (exemple macOS, JDK 17)
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# Verifier que le fichier de donnees est bien la
ls -lh data/datasets/yellow_tripdata_2024-01.parquet
```

### Si quelque chose coince

- **`JAVA_HOME is not set` ou `Unable to find a java executable`** : Java n'est pas installé ou
  pas dans le PATH. Installez un JDK 17 et exportez `JAVA_HOME`.
- **Beaucoup de lignes `WARN` au démarrage** : c'est normal, ce sont des avertissements. Le
  `setLogLevel("WARN")` réduit le bruit. Cherchez les vraies erreurs (`ERROR`).
- **`Path does not exist`** : vérifiez le chemin du Parquet. Lancez le script depuis la racine du
  projet pour que `data/datasets/...` soit valide, ou utilisez un chemin absolu.
- **La Spark UI ne s'affiche pas sur le port 4040** : une autre session occupe peut-être le port,
  Spark bascule alors sur 4041, 4042, etc. Lisez les logs de démarrage pour voir le port retenu.
