# Rapport de projet — ONISR (Accidents corporels)

- **Équipe** : [Noms des membres]
- **Jeu de données** : ONISR — Accidents corporels France (fichiers CSV 2023)
- **Date** : [date de rendu]

---

## Résumé

Ce rapport documente la transformation des données ONISR (bronze -> silver -> gold), les analyses réalisées (agrégations, jointures, fonctions fenêtrées), les optimisations appliquées et les enseignements métier. Le livrable officiel du projet est fourni sous la forme du fichier `rapport_projet_onsir.pdf` à la racine du dépôt.

---

## 1. Jeu de données et schéma cible

- Source : ONISR (fichiers CSV fournis dans `data/dataset/onsir/`)
- Volume : quelques centaines de milliers de lignes par table (varie selon l'année)
- Tables principales et schéma cible :
  - `caracteristiques` : `Num_Acc`, `an`, `mois`, `jour`, `heure`, `lum`, `agglo`, `atm`, `surf`, `descr_grav`, ...
  - `lieux` : `Num_Acc`, `com`, `dep`, `lat`, `long`, `catv`, ...
  - `vehicules` : `Num_Acc`, `num_veh`, `cat_veh`, ...
  - `usagers` : `Num_Acc`, `num_veh`, `sexe`, `situation`, `trajet`, `catu`, ...

- Questions métier ciblées :
  - Relations entre conditions météo (`atm`), état de la chaussée (`surf`) et gravité (`descr_grav`).
  - Répartition horaire et journalière des accidents.
  - Classement des départements par nombre d'accidents graves.
  - Profils d'usagers les plus exposés.

---

## 2. Pipeline (bronze -> silver -> gold)

Architecture générale :

```
CSV brut (bronze) -> nettoyage & normalisation (silver, Parquet) -> agrégations & exports (gold)
```

- Étapes principales :
  1. Lecture CSV avec schémas explicites (`StructType`) et encodage approprié.
  2. Nettoyage : cast des types, gestion des valeurs manquantes, normalisation des heures/jours, suppression des doublons (`dropDuplicates(['Num_Acc', ...])`).
  3. Enrichissements : dérivation de `jour_semaine`, buckets horaires, mapping des codes de gravité en labels.
  4. Écriture silver en Parquet partitionné par `dep` (et `an` si multi-années).
  5. Calculs gold : tables agrégées prêtes pour analyses et visualisations.

- Choix de partitionnement : `dep` pour permettre des sélections géographiques efficaces et réduire les octets lus.

---

## 3. Analyses

### Analyse 1 — Gravité vs météo / surface

- Question : Comment la gravité (`descr_grav`) varie-t-elle selon `atm` et `surf` ?
- Code clé (extrait) :

```python
from pyspark.sql import functions as F

res = (df_carac
    .groupBy('atm', 'surf')
    .agg(
        F.count('Num_Acc').alias('nb_accidents'),
        F.sum(F.when(F.col('descr_grav') == 2, 1).otherwise(0)).alias('nb_blessees'),
        F.sum(F.when(F.col('descr_grav') == 3, 1).otherwise(0)).alias('nb_tues')
    ).orderBy(F.desc('nb_accidents')))

res.show(10)
```

- Résumé des résultats : les conditions `clair/sèche` concentrent la majorité des accidents en volume; toutefois, la proportion d'accidents graves augmente sur chaussée humide/verglacée.

### Analyse 2 — Pics horaires et répartition par département

- Question : Quels départements et quais horaires présentent les plus forts volumes ?
- Code clé (extrait) :

```python
df = df_carac.join(df_lieux, on='Num_Acc', how='inner')
top = (df.groupBy('dep', 'heure')
       .agg(F.count('*').alias('nb'))
       .orderBy(F.desc('nb')))
top.show(20)
```

- Lecture métier : pics observés en heures de fin d'après-midi (trajets domicile-travail) avec forte concentration dans les départements urbains.

### Analyse 3 — Classement des départements (window)

- Question : Quels départements ont le plus fort taux d'accidents graves ?
- Code clé (extrait) :

```python
from pyspark.sql.window import Window

dep_stats = (df.groupBy('dep')
             .agg(
                 F.count('Num_Acc').alias('nb_acc'),
                 F.sum(F.when(F.col('descr_grav') == 3, 1).otherwise(0)).alias('nb_graves')
             )
             .withColumn('taux_graves', F.col('nb_graves') / F.col('nb_acc')))

window = Window.orderBy(F.desc('taux_graves'))
top_dep = dep_stats.withColumn('rang', F.row_number().over(window)).filter(F.col('rang') <= 10)
```

- Lecture métier : certains départements affichent un taux de gravité élevé malgré un volume modéré — prioriser diagnostics locaux.

---

## 4. Optimisations appliquées

- Principales optimisations :
  - Broadcast join pour tables de référence (gain sur shuffle et latence).
  - Cache des DataFrames réutilisés dans plusieurs étapes d'agrégation.
  - Écriture Parquet partitionnée pour pruning efficace.

- Impact observé : réduction notable des phases de shuffle et diminution du temps total d'exécution sur les étapes d'analyse (mesures et profils disponibles dans les logs de pipeline).

---

## 5. Observations Spark UI

- Points clés observés : les exchanges apparaissent principalement sur les `groupBy` et les jointures non-broadcast.
- Nombre de stages : dépend de la requête ; typiquement 3–6 pour un pipeline complet (lecture, join, aggregation).
- Recommandation : utiliser `explain()` et captures de la Spark UI pour valider l'effet des optimisations.

---

## 6. Exploration complémentaire

- Pistes testées : partition pruning sur `dep`, comparaison Parquet vs CSV, essais de `spark.sql.adaptive.enabled` (AQE) pour réduire skew.
- Protocole synthétique : fixer un workload, modifier une seule variable (partitionnement / AQE / broadcast) et mesurer temps & octets lus.

---

## 7. Limitations et améliorations futures

- Limites : encodage des CSV (`latin1`), qualité des géolocalisations manquantes, tests sur multi-années non exhaustifs.
- A améliorer : automatiser les tests de performance, construire un tableau de bord et étendre l'analyse multi-années.

---

## Annexes

- Fichiers importants : `data/dataset/onsir/` (CSV), scripts de pipeline : `pipeline_onsir.py`, rapports : `rapport_projet_onsir.pdf`.

Fin du rapport.
