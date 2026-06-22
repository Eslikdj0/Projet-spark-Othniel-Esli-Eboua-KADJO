# Espace stagiaire - Introduction à Apache Spark

Bienvenue. Ce dossier rassemble ce dont vous avez besoin pendant la formation.

## Avant de commencer

1. Installer PySpark (voir la racine du dépôt, section "Mise en route" du `README.md`) :
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install pyspark
   ```
2. Vérifier Java : `java -version` (Java 17 ou 21 ; Spark 4 requiert Java 17 minimum).
3. Télécharger les données : `bash data/download.sh`.

## Pendant le cours

- Les énoncés d'exercices sont dans `exercises/`. Vous pouvez aussi les ouvrir directement
  depuis les slides avec la touche `T` (panneau TP).
- Travaillez en binôme ou trinôme.
- N'hésitez pas à casser des choses : c'est comme ça qu'on apprend Spark.

## Programme

- Jour 1 : fondations, pourquoi Spark, RDD et évaluation paresseuse
- Jour 2 : DataFrames, Spark SQL, ingestion et formats
- Jour 3 : distribution, performance, Spark UI, ouverture (streaming, MLlib)
- Jour 4 : projet, un pipeline data de bout en bout

## Après le cours

- `data/sources-open-data.md` : des jeux de données pour continuer à pratiquer
- `projects/projet-jour-4.md` : un projet complet à refaire ou approfondir
- La documentation officielle : https://spark.apache.org/docs/latest/

Bon cours.
