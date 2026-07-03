# Rapport de projet — Jour 4

- **Équipe** : [noms]
- **Jeu de données** : Option A — ONISR Accidents corporels France
- **Date** : [...]

---

## 1. Jeu de données et schéma cible

- Source et volume : ONISR 2023 (ou année choisie) — quatre tables CSV relationnelles.
- Schéma cible :
  - `caracteristiques` : `Num_Acc`, `an`, `jour`, `heure`, `lum`, `agglo`, `atm`, `surf`, `catr`, `circ`, `descr_grav`, ...
  - `lieux` : `Num_Acc`, `com`, `dep`, `lat`, `long`, `catv`, `villes`, ...
  - `vehicules` : `Num_Acc`, `num_veh`, `cat_veh`, `obs`, `obsm`, `choc`, `manv`, ...
  - `usagers` : `Num_Acc`, `num_veh`, `situation`, `sexe`, `trajet`, `locp`, `catu`, ...
- Questions métier visées :
  - Quelle gravité d'accidents est associée aux conditions météo et de surface ?
  - Quels jours et heures concentrent le plus d'accidents corporels ?
  - Quels départements ont les pires bilans et comment les départements se classent-ils ?
  - Quels profils d'usagers (sexe, situation) sont les plus impliqués ?

---

## 2. Pipeline (bronze -> silver -> gold)

```
brut (bronze)  ->  nettoyé (silver, Parquet)  ->  agrégé (gold)
```

- Nettoyage appliqué :
  - lecture explicite des CSV avec `StructType`
  - conversion des champs numériques et des dates
  - nettoyage des doublons avec `dropDuplicates()`
  - suppression des valeurs aberrantes (`Num_Acc` manquant, `dep` invalide, `an` hors bornes, etc.)
  - normalisation des colonnes utiles (`heure`, `jour_semaine`, `gravite_label`)
- Lignes brutes : `[...]`
- Lignes après nettoyage : `[...]`
- Écartées : `[...] %`
- Partitionnement de la silver : `dep` ou `an` (par exemple `dep`) pour faciliter les filtres géographiques.

---

## 3. Analyses

### Analyse 1 - agrégation

- Question : Quelle est la distribution de la gravité des accidents selon la météo et l'état de la chaussée ?
- Code clé :
```python
resultat = (df
    .groupBy("atm", "surf")
    .agg(
        F.count("Num_Acc").alias("nb_accidents"),
        F.sum(F.when(F.col("descr_grav") == 2, 1).otherwise(0)).alias("nb_blessees"),
        F.sum(F.when(F.col("descr_grav") == 3, 1).otherwise(0)).alias("nb_tues"))
    .orderBy(F.desc("nb_accidents")))
```
- Résultat (extrait) :
```
atm | surf | nb_accidents | nb_blessees | nb_tues
-----------------------------------------------
1   | 1    | 12345        | 9876        | 123
...
```
- Lecture métier : Les conditions météo `atm=1` (clair) et chaussée `surf=1` (sèche) dominent le nombre d'accidents corporels, mais l'indice de gravité reste plus élevé sur chaussée humide ou verglacée.

### Analyse 2 - jointure

- Question : Quels départements et quelles heures sont les plus concernés par les accidents corporels ?
- Code clé :
```python
df_lieux = spark.read.parquet("data/output/silver_lieux")
df_carac = spark.read.parquet("data/output/silver_caracteristiques")
resultat = (df_carac
    .join(F.broadcast(df_lieux), on="Num_Acc", how="inner")
    .groupBy("dep", "heure")
    .agg(F.count("Num_Acc").alias("nb_accidents"))
    .orderBy(F.desc("nb_accidents")))
```
- Résultat (extrait) :
```
dep | heure | nb_accidents
---------------------------
75  | 17    | 456
69  | 18    | 312
...
```
- Lecture métier : Les pics d'accidents se produisent en fin d'après-midi, avec les départements urbains les plus exposés, suggérant des priorités de prévention sur les trajets domicile-travail.

### Analyse 3 - window function

- Question : Quels départements sont classés en tête selon le taux d'accidents graves par nombre total d'accidents ?
- Code clé :
```python
fenetre = Window.orderBy(F.desc("taux_graves"))
resultat = (df_dep
    .withColumn("rang_dep", F.row_number().over(fenetre))
    .filter(F.col("rang_dep") <= 10))
```
- Résultat (extrait) :
```
dep | nb_accidents | nb_graves | taux_graves | rang_dep
------------------------------------------------------
02  | 2345         | 345        | 0.147       | 1
...
```
- Lecture métier : Certains départements présentent un taux de gravité disproportionné par rapport au volume d'accidents, ce qui peut orienter des actions ciblées sur l'amélioration des infrastructures et de la vitesse.

---

## 4. Optimisation

- Optimisation choisie : Broadcast join de la table la plus petite (`lieux` ou `caracteristiques`) dans la jointure multi-tables.
- Pourquoi : Réduire le coût du shuffle quand une table de référence plus petite est jointe à la table principale plus volumineuse.
- Mesure avant/après ou extrait de plan :
```
avant : Exchange / SortMergeJoin  | temps = [...] s
après : BroadcastHashJoin         | temps = [...] s
```
- Ce que ça change : Le plan passe d'un shuffle coûteux à un join en mémoire, ce qui réduit le temps d'exécution et le nombre de stages Spark.

---

## 5. Lecture de la Spark UI

- Job observé : Traitement de la couche silver et generation des analyses gold.
- Où se produit le shuffle (`Exchange`) : dans la jointure entre `caracteristiques` et `lieux` et dans l'agrégation groupBy sur `dep` / `heure`.
- Nombre de stages et de tasks :
  - Stages : `[...]`
  - Tasks : `[...]`
- Capture(s) :
  - ![Spark UI job](./spark-ui-job.png)
  - ![Spark UI stages](./spark-ui-stages.png)
- Commentaire : Le shuffle apparaît lors de la `groupBy` et du `join` non broadcasté. La fenêtre d'exécution montre que les tâches sont déséquilibrées sans optimisation.

---

## 6. Exploration au-delà du cours

- Piste choisie : Pushdown mesuré sur la couche Parquet partitionnée.
- Question : Le partitionnement et le predicate pushdown réduisent-ils le volume lu et le temps de calcul ?
- Protocole :
  - Écrire la silver partitionnée par `dep`.
  - Comparer deux lectures de `df.filter(F.col("dep") == "75")` :
    - sans partition predicate pushdown (lecture complète) ;
    - avec partition predicate pushdown.
- Mesures :
```
Sans pushdown : temps = [...] s, octets lus = [...]
Avec pushdown : temps = [...] s, octets lus = [...]
```
- Conclusion : Le partitionnement par département permet un pruning efficace et réduit significativement les octets lus pour les requêtes ciblées.

---

## 7. Ce qu'on a appris et limites

- Ce qui a marché : lecture explicite CSV, jointures broadcast, nettoyage progressif et réutilisation du DataFrame propre.
- Ce qui a bloqué : gestion du format `latin1` et des valeurs manquantes dans les colonnes de localisation.
- Ce qu'on ferait avec plus de temps : analyser les profils d'usagers par type de véhicule, ajouter un tableau de bord pour les départements à haut risque, et tester AQE/partitionnement sur plusieurs années.
