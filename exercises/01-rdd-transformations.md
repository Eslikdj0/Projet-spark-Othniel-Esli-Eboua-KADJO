# RDD : transformations, actions et évaluation paresseuse

## Objectif

Manipuler des RDD avec les opérations de base (`parallelize`, `map`, `filter`, `flatMap`,
`reduceByKey`), écrire un word count, et surtout comprendre la distinction fondamentale entre
les transformations (paresseuses) et les actions (qui déclenchent le calcul). À la fin de ce TP,
vous savez observer la paresse de Spark et expliquer pourquoi rien ne s'exécute tant qu'aucune
action n'est appelée.

## Contexte

Le RDD (Resilient Distributed Dataset) est la brique de base de Spark. En pratique, on préfère le
DataFrame (que l'on verra dès le prochain TP), mais le RDD est le meilleur support pour comprendre
le modèle d'exécution : immutabilité, distribution en partitions, évaluation paresseuse et lineage.

On reste sur notre fil rouge, les courses de taxi jaunes de New York (NYC TLC). Pour rester au
niveau du RDD bas niveau, on commence par de petites collections Python que l'on distribue avec
`parallelize`, puis on enchaîne sur un word count classique, et enfin on accède au RDD sous-jacent
du DataFrame taxi (`yellow_tripdata_2024-01.parquet`) pour faire quelques comptages à la main. On
suppose les fichiers présents dans `data/datasets/`.

L'objectif n'est pas de battre des records de performance (le DataFrame fera mieux), mais de
sentir comment Spark assemble un plan de calcul et ne l'exécute qu'au dernier moment.

## Consignes

Créez un fichier `tp01_rdd.py`. Démarrez par la session habituelle, puis récupérez le
`SparkContext` qui sert d'entrée pour les RDD :

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP01 - RDD et paresse")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext     # point d'entrée pour les RDD
```

### 1. Créer un RDD et compter les partitions

Distribuez une liste de nombres avec `parallelize` et observez le nombre de partitions. Le nombre
de partitions correspond au nombre d'unités de travail traitables en parallèle.

```python
nombres = sc.parallelize(range(1, 21), numSlices=4)

# À compléter : afficher le nombre de partitions
print("Partitions :", nombres.________())

# À compléter : ramener tous les éléments vers le driver (action)
print("Contenu :", nombres.________())
```

Question : `parallelize` est-il une transformation ou une action ? Et `collect` ?

### 2. map et filter

Calculez le carré de chaque nombre, puis ne gardez que les carrés pairs. Enchaînez les
transformations, puis déclenchez le calcul avec une action.

```python
carres = nombres.map(lambda x: x * x)
carres_pairs = carres.filter(lambda x: x % 2 == 0)

# À compléter : récupérer le résultat (action)
print("Carres pairs :", carres_pairs.________())
```

À ce stade, demandez-vous : à quel moment précis Spark a-t-il réellement calculé quelque chose ?

### 3. Observer la paresse

Insérez un effet de bord (un `print`) dans la fonction passée à `map`, puis construisez la chaîne
de transformations SANS appeler d'action. Constatez que rien ne s'affiche.

```python
def trace(x):
    print("  -> je calcule", x)     # effet de bord visible
    return x * 10

paresseux = nombres.map(trace)
print("La transformation est definie, mais rien ne s'est encore affiche.")

# À compléter : maintenant, déclenchez une action et observez les traces apparaître
resultat = paresseux.________()
print("Resultat :", resultat)
```

Notez l'ordre des messages dans la console : la phrase "rien ne s'est encore affiche" doit sortir
AVANT les lignes "-> je calcule".

### 4. flatMap : aplatir

`map` renvoie un élément par élément d'entrée. `flatMap` renvoie zéro, un ou plusieurs éléments
par élément d'entrée, puis aplatit le tout. Comparez les deux sur des phrases.

```python
phrases = sc.parallelize([
    "le taxi jaune roule",
    "le taxi attend le client",
])

# map : une liste de mots par phrase (resultat imbrique)
par_map = phrases.map(lambda p: p.split(" "))

# flatMap : tous les mots a plat
par_flatmap = phrases.________(lambda p: p.split(" "))

print("map      :", par_map.collect())
print("flatMap  :", par_flatmap.collect())
```

### 5. Word count avec reduceByKey

Assemblez le pipeline classique du word count : aplatir en mots, transformer chaque mot en couple
`(mot, 1)`, puis sommer par clé avec `reduceByKey`.

```python
texte = sc.parallelize([
    "le taxi jaune roule dans la ville",
    "le client appelle le taxi jaune",
    "la ville dort le taxi roule",
])

comptage = (
    texte
    .flatMap(lambda ligne: ligne.split(" "))   # aplatir en mots
    .map(lambda mot: (mot, 1))                  # À compléter si besoin : couple (mot, 1)
    .________(lambda a, b: a + b)               # À compléter : sommer par clé
)

# Trier par frequence decroissante et afficher le top 5
top = comptage.sortBy(lambda couple: couple[1], ascending=False).take(5)
for mot, n in top:
    print(f"{mot:10s} {n}")
```

### 6. Du DataFrame taxi vers le RDD

Chargez le Parquet taxi en DataFrame, puis descendez sur son RDD sous-jacent (`df.rdd`) pour
compter à la main le nombre de courses par nombre de passagers. C'est exactement un word count,
mais la clé est `passenger_count`.

```python
chemin = "data/datasets/yellow_tripdata_2024-01.parquet"
df = spark.read.parquet(chemin).select("passenger_count")

comptage_passagers = (
    df.rdd
    .map(lambda row: (row["passenger_count"], 1))   # couple (nb_passagers, 1)
    .________(lambda a, b: a + b)                    # À compléter : sommer par clé
    .sortByKey()
)

for nb_passagers, n in comptage_passagers.collect():
    print(nb_passagers, "->", n)
```

Comparez mentalement avec ce que ferait un simple `df.groupBy("passenger_count").count()` : même
résultat, mais le DataFrame est plus lisible et plus rapide. Le RDD sert ici à comprendre le
mécanisme, pas à être la meilleure solution.

### 7. Bonus : compter sans tout ramener

Au lieu de `collect()` (qui ramène tout sur le driver, dangereux sur de gros volumes), utilisez
des actions qui renvoient un résultat agrégé : `count()`, `take(n)`, `first()`, `countByKey()`.
Trouvez le nombre total de mots distincts du texte du point 5 sans jamais appeler `collect()`.

```python
nb_mots_distincts = comptage.________()   # une action qui ne ramene pas tout
print("Mots distincts :", nb_mots_distincts)
```

N'oubliez pas de fermer la session à la fin :

```python
spark.stop()
```

## Livrable

Vous avez réussi le TP si :

- Le script s'exécute sans erreur de la session jusqu'au `spark.stop()`.
- Vous savez dire, pour chaque opération utilisée, si c'est une transformation (paresseuse) ou une
  action (déclenche le calcul).
- Au point 3, les traces "-> je calcule" apparaissent APRÈS la phrase annonçant que la
  transformation est seulement définie. Vous pouvez l'expliquer.
- Le word count du point 5 affiche un top correct : `le` arrive en tête (4 occurrences), suivi de
  `taxi`, `la`, `roule`, `ville` (autour de 2 à 3 occurrences chacun).
- Le comptage par nombre de passagers (point 6) donne des résultats de l'ordre de plusieurs
  millions de courses au total, avec une nette majorité pour `1` passager.
- Au point 7, vous obtenez le nombre de mots distincts sans utiliser `collect()`.

## Aide

### Transformation ou action ?

- **Transformations (paresseuses)** : `map`, `filter`, `flatMap`, `reduceByKey`, `sortBy`,
  `sortByKey`, `distinct`. Elles renvoient un nouveau RDD et ne calculent rien tout de suite.
  Elles ne font qu'ajouter une étape au plan (le lineage).
- **Actions (déclenchent le calcul)** : `collect`, `count`, `take`, `first`, `reduce`,
  `countByKey`, `saveAsTextFile`. Elles renvoient un résultat Python (ou écrivent un fichier) et
  forcent l'exécution de toute la chaîne de transformations en amont.
- Règle à retenir : tant qu'on enchaîne des transformations, rien ne tourne. La première action
  rencontrée déclenche tout le pipeline.

### Rappels d'API RDD

- `sc.parallelize(collection, numSlices=N)` : crée un RDD à partir d'une collection Python, en
  `N` partitions.
- `rdd.getNumPartitions()` : nombre de partitions.
- `rdd.map(f)` : applique `f` à chaque élément (un en entrée, un en sortie).
- `rdd.flatMap(f)` : applique `f` puis aplatit (un en entrée, zéro à plusieurs en sortie).
- `rdd.filter(predicat)` : garde les éléments pour lesquels `predicat` renvoie `True`.
- `rdd.reduceByKey(f)` : sur un RDD de couples `(cle, valeur)`, combine les valeurs d'une même clé
  avec `f`. C'est une transformation, mais elle provoque un shuffle (redistribution par clé).
- `rdd.sortBy(cle, ascending=False)` et `rdd.sortByKey()` : tri.
- `rdd.collect()` : ramène tout au driver (à éviter sur de gros volumes).
- `rdd.count()`, `rdd.take(n)`, `rdd.first()`, `rdd.countByKey()` : actions agrégées, sans tout
  ramener.

### Accéder au RDD d'un DataFrame

- `df.rdd` donne un RDD de `Row`. On accède à une colonne avec `row["nom_colonne"]` ou
  `row.nom_colonne`.
- En pratique, on ne fait presque jamais cela : `df.groupBy("col").count()` est plus lisible et
  plus rapide. Ici c'est volontaire, pour le mécanisme.

### Pièges à anticiper

- `print` dans une fonction `map` peut apparaître dans les logs des executors plutôt que dans
  votre console selon le mode. En `local[*]`, ils s'affichent bien dans votre terminal : c'est ce
  qui rend la démonstration de la paresse visible.
- `collect()` sur le RDD complet du Parquet taxi (3 millions de lignes) peut saturer la mémoire du
  driver. Pour le comptage par passagers, on agrège AVANT (`reduceByKey`), donc seul un petit
  résultat revient : c'est sûr.

### Commandes utiles

```bash
# Lancer le script depuis la racine du projet
python tp01_rdd.py

# Verifier que le Parquet est bien la
ls -lh data/datasets/yellow_tripdata_2024-01.parquet
```
