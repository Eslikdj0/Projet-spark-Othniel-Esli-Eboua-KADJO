# Syllabus - Introduction à Apache Spark

Ingestion de données et calcul distribué avec PySpark.

## Présentation

Apache Spark est le moteur de référence pour traiter de gros volumes de données de manière
distribuée. Cette formation de quatre jours (trois jours de cours et un jour de projet) donne
les bases solides pour comprendre, écrire et optimiser des traitements Spark, aussi bien pour
ingérer des données que pour les calculer à grande échelle.

Le cours est orienté pratique : une démonstration ou un exercice suit immédiatement chaque
notion. Le ratio visé est d'environ un tiers de magistral pour deux tiers de pratique.

## Public visé

- Développeuses et développeurs souhaitant monter en compétence sur la donnée
- Futurs data engineers et data analysts
- Profils techniques amenés à manipuler de gros volumes de données

## Prérequis

- Programmation Python : variables, fonctions, listes, dictionnaires, modules
- Notions de SQL : SELECT, WHERE, GROUP BY, JOIN
- Aisance avec la ligne de commande et un terminal
- Connaître un éditeur de code ou un notebook

Aucune connaissance préalable de Spark, de Hadoop ou du calcul distribué n'est requise.

## Durée et organisation

- 3 jours de cours, 7 heures par jour
- 1 jour de projet encadré (7 heures), le quatrième jour
- Ratio cible : 1/3 magistral, 2/3 pratique
- Travail en binômes ou trinômes pour les exercices

## Langue et outils

- Langue : français
- Langage : Python (PySpark). Le langage Scala est mentionné quand c'est utile, mais tous les
  exercices sont en Python.
- Version : Apache Spark 4.x (4.0 ou supérieur ; version stable courante 4.1 en 2026), exécuté en mode local sur les machines des stagiaires
- Données : jeux d'open data réels (voir `data/sources-open-data.md`), avec comme fil rouge
  principal les courses de taxi de New York au format Parquet.

## Compétences visées (à la fin de la formation)

À l'issue du cours, la stagiaire ou le stagiaire est capable de :

1. Expliquer pourquoi Spark existe, son histoire et sa place dans l'écosystème data.
2. Décrire l'architecture d'une application Spark : driver, executors, cluster manager.
3. Distinguer RDD, DataFrame et Dataset, et savoir lequel utiliser.
4. Comprendre l'évaluation paresseuse, les transformations et les actions, le lineage.
5. Ingérer des données depuis plusieurs sources et formats (CSV, JSON, Parquet, JDBC).
6. Manipuler des DataFrames : sélection, filtrage, colonnes dérivées, agrégations, jointures.
7. Écrire des requêtes en Spark SQL et combiner SQL et API DataFrame.
8. Utiliser les fonctions intégrées, les UDF et les window functions.
9. Comprendre comment Spark exécute un job : jobs, stages, tasks, shuffle.
10. Diagnostiquer et optimiser un traitement : partitionnement, cache, broadcast join, AQE.
11. Lire la Spark UI pour comprendre et améliorer un job.
12. Avoir une première expérience de Structured Streaming et de MLlib.
13. Connaître les modes de déploiement et l'usage de Spark en entreprise.
14. Concevoir et réaliser un pipeline ETL complet de bout en bout sur de l'open data.

---

## Notions et objectifs par jour

### Jour 1 - Fondations : pourquoi et comment Spark calcule

Notions :
- Le problème du Big Data : les 3 V (volume, vélocité, variété), scale-up contre scale-out
- Histoire : MapReduce, Hadoop, naissance de Spark à Berkeley (AMPLab, 2009), passage à Apache
- Pourquoi Spark : exécution en mémoire, graphe d'exécution (DAG), comparaison avec MapReduce
- Architecture : driver, executors, cluster manager, partitions
- SparkSession et SparkContext
- RDD : immutabilité, évaluation paresseuse, transformations contre actions, lineage, tolérance aux pannes
- Du RDD au DataFrame : Catalyst et Tungsten, pourquoi le DataFrame est préférable

Objectifs pédagogiques :
- Savoir expliquer en une minute ce qu'est Spark et à quel problème il répond
- Installer PySpark et lancer une première SparkSession en local
- Écrire un premier traitement (word count) et lire un premier jeu de données
- Comprendre pourquoi rien ne s'exécute tant qu'aucune action n'est appelée

### Jour 2 - Manipuler et ingérer : DataFrame API, Spark SQL et formats

Notions :
- L'API DataFrame en profondeur : select, filter, withColumn, drop, agrégations, groupBy, jointures
- Le type Column et les expressions, le schéma (inférence contre schéma explicite)
- Spark SQL : vues temporaires, requêtes SQL, équivalence avec l'API DataFrame
- Spark comme outil d'ingestion : lecture et écriture, sources et destinations
- Formats de fichiers : CSV, JSON, Parquet, ORC, et pourquoi le colonnaire change tout
- Partitionnement à l'écriture, modes d'écriture, lecture par JDBC
- Fonctions intégrées, fonctions définies par l'utilisateur (UDF), window functions
- Qualité des données : valeurs manquantes, doublons, nettoyage et typage

Objectifs pédagogiques :
- Construire un pipeline d'ingestion qui lit, nettoie et réécrit en Parquet partitionné
- Écrire les mêmes traitements en API DataFrame et en Spark SQL
- Choisir le bon format de fichier selon le besoin et justifier ce choix
- Utiliser des window functions pour des calculs par groupe (classement, cumul, moyenne glissante)

### Jour 3 - Distribution et performance : exécution, optimisation et ouverture

Notions :
- Anatomie de l'exécution : job, stage, task, et leur rapport au code
- Transformations étroites contre larges, rôle du shuffle
- Partitions : repartition contre coalesce, nombre de partitions, données mal réparties (skew)
- Cache et persistance : niveaux de stockage, quand mettre en cache
- Optimisation : broadcast join, Adaptive Query Execution (AQE), predicate pushdown, partition pruning
- Lire la Spark UI : DAG, stages, tâches lentes, lecture du plan d'exécution
- Structured Streaming : modèle micro-batch, source, sink, watermark (introduction et démo)
- MLlib : pipeline de machine learning, exemple simple (introduction)
- Déploiement : spark-submit, modes client et cluster, Spark sur YARN et Kubernetes
- Spark en entreprise : Databricks, EMR, Dataproc, Delta Lake, intégration avec Kafka et Airflow
- Nouveautés Spark 4 : mode ANSI SQL activé par défaut, Spark Connect (client léger), type VARIANT, API Python Data Source

Objectifs pédagogiques :
- Lire un plan d'exécution et identifier le shuffle dans la Spark UI
- Accélérer un traitement avec le cache, un broadcast join et un bon partitionnement
- Comprendre la différence entre traitement par lots et traitement en flux
- Situer Spark dans une architecture data d'entreprise

### Jour 4 - Projet : un pipeline data de bout en bout

Notions mobilisées : tout le cours, appliquées à un jeu d'open data au choix.

Objectifs pédagogiques :
- Concevoir et réaliser un pipeline complet : ingestion, nettoyage, transformation, agrégation, analyse
- Justifier les choix techniques (format, partitionnement, optimisations)
- Lire la Spark UI pour expliquer le comportement du job
- Présenter le résultat et la démarche en fin de journée

---

## Modalités d'évaluation

- Exercices pratiques tout au long des trois jours (auto-évaluation guidée grâce à la section Aide de chaque exercice)
- Projet du jour 4 : pipeline complet sur open data, évalué sur une grille (voir `projects/`)
- Restitution orale du projet en fin de quatrième journée

## Matériel et environnement

- Un poste par stagiaire avec Python 3.9 ou supérieur et Java 17 ou 21 installés (Spark 4 requiert Java 17 minimum)
- PySpark installé via `pip install pyspark` (mode local, pas de cluster requis)
- Optionnel : Jupyter ou un notebook, ou simplement un terminal et un éditeur
- Accès internet pour télécharger les jeux d'open data (voir `data/download.sh`)
- Espace disque conseillé : au moins 5 Go libres pour les données

Voir `README.md` pour la mise en route.
