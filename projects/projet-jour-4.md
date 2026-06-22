# Projet - Pipeline data de bout en bout avec Spark

Jour 4 de la formation Apache Spark. Cette journée est consacrée à un projet pratique : concevoir
et réaliser un pipeline complet, de l'ingestion des données brutes jusqu'à l'analyse, en mobilisant
tout ce qui a été vu pendant les trois premiers jours.

Le projet se fait en binôme ou trinôme. Le code est en PySpark, exécuté en mode local sur vos
machines. L'objectif n'est pas de produire le pipeline le plus complexe, mais un pipeline propre,
qui tourne, qui répond à de vraies questions, et dont vous savez expliquer le comportement.

---

## 1. Objectif et socle minimal attendu

Vous construisez un pipeline ETL (Extract, Transform, Load) suivi d'une phase d'analyse. La
démarche est plus importante que le nombre de lignes de code : on attend un pipeline lisible,
reproductible, et dont chaque étape est justifiée.

Le socle minimal, attendu de toutes les équipes, comporte cinq éléments :

1. **Une ingestion propre** : lire les données brutes (Parquet ou CSV), poser un schéma (explicite
   pour le CSV), typer correctement les colonnes, nettoyer (valeurs manquantes, doublons, valeurs
   aberrantes), puis écrire une couche intermédiaire en Parquet. C'est la base : si l'ingestion est
   sale, toute l'analyse est faussée.
2. **Au moins trois analyses** distinctes qui répondent à des questions métier. Au moins une doit
   utiliser une agrégation (`groupBy` + `agg`), au moins une doit utiliser une jointure, et au moins
   une doit utiliser une window function (classement, cumul ou moyenne glissante).
3. **Une optimisation justifiée** : un broadcast join, une mise en cache d'un DataFrame réutilisé,
   ou un repartitionnement réfléchi. Vous devez pouvoir dire pourquoi vous l'avez fait et ce que ça
   a changé (mesure de temps avant et après, ou lecture du plan).
4. **Une lecture de la Spark UI** : ouvrir l'interface (port 4040), repérer un job avec shuffle,
   lire le DAG, et savoir commenter ce que vous voyez (stages, tasks, échange de données).
5. **Une courte restitution** : présenter en 10 minutes votre jeu de données, vos choix, vos
   résultats et ce que vous avez appris.

> Tout ce qui dépasse ce socle (streaming, MLlib, multi-mois, partitionnement fin, Delta) est un
> bonus valorisé mais facultatif. Mieux vaut un socle solide qu'un bonus bancal posé sur un socle
> fragile.

---

## 2. Choisir son jeu de données

Quatre options, toutes décrites dans `data/sources-open-data.md` (URLs vérifiées). Le script
`data/download.sh` récupère directement le taxi, les zones, un département DVF et MovieLens ; pour
les accidents ONISR, suivez le lien donné dans `data/sources-open-data.md`. Choisissez l'option qui
vous parle : un projet sur des données
qui vous intéressent se mène mieux.

### Option A : NYC Yellow Taxi multi-mois (recommandé)

Le fil rouge du cours, en plus gros. Au lieu d'un seul mois, vous travaillez sur 3 mois ou plus
(`yellow_tripdata_2024-01.parquet`, `2024-02`, `2024-03`), plus la table des zones
(`taxi_zone_lookup.csv`).

- Source : section 1 de `data/sources-open-data.md`.
- Format : Parquet natif (idéal pour parler de colonnaire, predicate pushdown, partition pruning).
- Volume : environ 3 millions de courses par mois, donc ~9 millions sur 3 mois. De quoi sentir le
  parallélisme et le shuffle.
- Idées d'analyses : revenu par heure de la journée et par jour de semaine ; top 10 des trajets
  zone à zone (jointure double sur `PULocationID` et `DOLocationID`) ; classement des zones par
  pourboire moyen (window) ; évolution mois après mois ; détection d'anomalies de tarif.
- Optimisation évidente : broadcast de la table des zones (265 lignes) dans la jointure.

### Option B : DVF, demandes de valeurs foncières (immobilier France)

Toutes les transactions immobilières d'un département. Excellent pour un public francophone.

- Source : section 2 de `data/sources-open-data.md`. Commencer par un seul département
  (`dvf_75_2023.csv.gz` ou un autre département, beaucoup plus léger que la France entière).
- Format : CSV compressé gzip. Bon cas pour un schéma explicite et un nettoyage sérieux.
- Colonnes clés : `date_mutation`, `valeur_fonciere`, `code_postal`, `nom_commune`,
  `code_departement`, `type_local`, `surface_reelle_bati`, `nombre_pieces_principales`.
- Idées d'analyses : prix au m2 par commune (calcul `valeur_fonciere / surface_reelle_bati`,
  attention à la division par zéro) ; classement des communes les plus chères (window) ; évolution
  mensuelle des prix ; répartition par `type_local` (maison, appartement) ; filtrage des valeurs
  aberrantes (transactions à 1 euro, surfaces nulles).
- Optimisation évidente : cache du DataFrame nettoyé, réutilisé par plusieurs analyses.

### Option C : Accidents corporels ONISR (sécurité routière France)

Tous les accidents corporels déclarés en France pour une année, répartis en quatre fichiers
relationnels. Le meilleur choix pour s'exercer aux jointures multi-tables.

- Source : section 3 de `data/sources-open-data.md`. Quatre CSV par année : `caracteristiques`,
  `lieux`, `vehicules`, `usagers`.
- Format : CSV, séparateur point-virgule, encodage à vérifier (souvent latin1). Schéma explicite
  fortement conseillé.
- Clé de jointure : `Num_Acc` relie les quatre tables.
- Idées d'analyses : gravité des accidents par condition météo ; accidents par heure et jour de la
  semaine ; classement des départements (window) ; profils d'usagers (âge, catégorie) ; jointure
  des quatre tables pour un tableau croisé complet.
- Optimisation évidente : broadcast de la plus petite table dans une jointure, ou cache de la table
  jointe réutilisée.

### Option D : MovieLens (notes de films)

Le classique des jointures, agrégations et recommandation. Utile si vous voulez tenter le bonus
MLlib.

- Source : section 5 de `data/sources-open-data.md`. Commencer par `ml-latest-small`
  (`ratings.csv`, `movies.csv`), passer à `ml-25m` si la machine suit.
- Format : CSV. Jointure `ratings` + `movies` sur `movieId`.
- Idées d'analyses : films les mieux notés (avec un seuil minimal de votes pour éviter les biais) ;
  popularité par genre ; nombre de notes par utilisateur ; note moyenne dans le temps ; classement
  des films par genre (window).
- Optimisation évidente : broadcast de `movies` (petit) dans la jointure avec `ratings` (gros).
- Bonus naturel : recommandation par ALS (MLlib).

---

## 3. Étapes attendues

Le pipeline suit l'architecture vue en cours : couche brute (bronze), couche nettoyée (silver),
couche agrégée (gold). Vous n'êtes pas obligés d'écrire physiquement les trois couches, mais la
logique doit être présente.

### Étape 1 : ingestion et nettoyage (couche bronze vers silver)

- Lire les données brutes. Pour du CSV, définir un schéma explicite (`StructType`) plutôt que de
  laisser Spark inférer : c'est plus sûr et plus rapide.
- Inspecter : `printSchema()`, `show(5)`, `count()`, `describe()` sur les colonnes numériques.
- Typer correctement (dates, nombres). Convertir les colonnes texte qui devraient être numériques.
- Nettoyer : retirer les doublons (`dropDuplicates`), gérer les valeurs manquantes
  (`na.drop` / `na.fill`), filtrer les valeurs aberrantes (montants négatifs, distances nulles,
  surfaces à zéro, dates incohérentes).
- Ecrire la couche nettoyée en Parquet (`write.mode("overwrite").parquet(...)`), éventuellement
  partitionnée par une colonne pertinente (mois, département, année).

### Étape 2 : transformation et analyse (couche silver vers gold)

- Relire la couche Parquet nettoyée (pas les données brutes : on travaille sur du propre).
- Construire vos colonnes dérivées (`withColumn`) : durée, prix au km, prix au m2, heure de la
  journée, jour de semaine, catégorie.
- Réaliser vos trois analyses : une agrégation, une jointure, une window function.
- Introduire votre optimisation : broadcast join, cache, ou repartition, en mesurant l'effet.

### Étape 3 : finalisation

- Ecrire les résultats des analyses (Parquet ou CSV de synthèse, petits fichiers).
- Ouvrir la Spark UI (port 4040) pendant qu'un job tourne, repérer un stage avec shuffle, lire le
  DAG, noter ce que vous observez.
- Nettoyer le code, ajouter des commentaires, préparer la restitution.

---

## 4. Jalons horaires de la journée

Les horaires sont indicatifs, l'important est
de respecter l'ordre et de ne pas sauter le cadrage.

| Horaire | Bloc | Ce que vous faites |
|---------|------|--------------------|
| 9h30    | Cadrage (30 min) | Rappel du socle, choix du jeu de données, constitution des équipes |
| 10h00   | Conception (45 min) | Schéma cible, étapes du pipeline, liste des questions métier à traiter |
| 10h45   | Réalisation étape 1 (jusqu'à 13h00) | Ingestion, typage, nettoyage, écriture de la couche intermédiaire |
| 13h00   | Pause déjeuner | - |
| 14h00   | Réalisation étape 2 (90 min) | Agrégations, jointures, window functions, optimisation |
| 15h30   | Réalisation étape 3 (60 min) | Sorties finales, lecture de la Spark UI, préparation de la restitution |
| 16h30   | Restitutions (45 min) | 10 min par équipe, retour sur les choix, bilan des 4 jours |

> Visez une couche intermédiaire écrite et relue avant la pause déjeuner : vous attaquerez
> l'analyse de l'après-midi l'esprit tranquille.

---

## 5. Livrables

A rendre en fin de journée (un dépôt ou un dossier partagé par équipe) :

- **Les scripts PySpark** du pipeline, organisés et commentés. Au minimum un script d'ingestion et
  un script d'analyse, ou un pipeline unique découpé en sections claires. Vous pouvez partir du
  squelette fourni dans `starter-code/pipeline.py`.
- **Les sorties** : la couche Parquet nettoyée et les fichiers de résultats des analyses (les
  petits fichiers de synthèse, pas les données brutes).
- **Une courte présentation** (quelques slides ou un README) : le jeu de données choisi, le schéma
  cible, les trois analyses et leurs résultats, l'optimisation et son effet, une capture ou une
  description de ce que vous avez lu dans la Spark UI.

---

## 6. Grille d'évaluation

Notée sur 20 points. La grille récompense un socle complet et bien expliqué avant la complexité.

| Critere | Points | Attendu |
|---------|--------|---------|
| Ingestion et nettoyage | 4 | Schéma correct (explicite pour le CSV), typage juste, doublons et valeurs aberrantes traités, couche intermédiaire écrite en Parquet. |
| Analyses (3 minimum) | 5 | Au moins une agrégation, une jointure et une window function. Résultats cohérents et qui ont du sens métier. |
| Optimisation justifiée | 4 | Une optimisation réelle (broadcast, cache, repartition) avec une explication du pourquoi et une mesure ou une lecture de plan à l'appui. |
| Lecture de la Spark UI | 3 | Capacité à ouvrir la Spark UI, repérer un shuffle, lire le DAG et commenter stages et tasks. |
| Qualité du code | 2 | Code lisible, découpé en étapes, commenté, reproductible. Pas de `collect()` inutile sur de gros volumes. |
| Restitution | 2 | Présentation claire en 10 min : démarche, choix, résultats, ce qui a été appris. |
| **Total** | **20** | |

> Bonus possible : jusqu'à 2 points supplémentaires (plafonnés à 20/20) pour une piste avancée
> réussie et bien expliquée.

---

## 7. Pistes bonus

A tenter seulement si le socle est solide et qu'il vous reste du temps :

- **Structured Streaming** : simuler un flux en déposant des fichiers dans un dossier surveillé par
  `readStream`, puis écrire un agrégat continu. Sur le taxi, on peut découper un mois en plusieurs
  fichiers et les laisser arriver un par un.
- **MLlib** : un mini pipeline de machine learning. Sur le taxi, prédire le pourboire à partir de la
  distance et du montant (régression). Sur MovieLens, une recommandation par ALS.
- **Partitionnement fin** : écrire la couche nettoyée partitionnée (`partitionBy`) par mois ou par
  département, puis montrer le partition pruning à la relecture (lire le plan, comparer les temps).
- **Delta Lake** : utiliser le format Delta à la place du Parquet pour la couche intermédiaire, et
  montrer une mise à jour transactionnelle (`MERGE` ou `update`) plutôt qu'une réécriture complète.

---

## 8. Conseils et pièges à éviter

- **Commencez petit.** Un département DVF, un mois de taxi, MovieLens small. Faites tourner le
  pipeline de bout en bout sur un petit volume avant d'ajouter des mois ou des années. Un pipeline
  qui marche sur 100 000 lignes marchera sur 10 millions.
- **Validez le schéma tôt.** Un `printSchema()` dès le début évite de découvrir en fin de journée
  qu'une colonne numérique a été lue comme du texte (fréquent en CSV).
- **Ne collectez pas tout.** `collect()` ou `toPandas()` ramène toutes les données sur le driver et
  peut le faire planter. Utilisez `show()`, `take(n)`, ou écrivez sur disque. On ne collecte que de
  petits résultats agrégés.
- **Méfiez-vous de la division par zéro.** En Spark, `x / 0` ne plante pas : il renvoie `Infinity`
  ou `NaN`, qui pollue toutes les moyennes ensuite. Protégez le dénominateur avec un
  `F.when(col > 0, ...).otherwise(None)`.
- **Utilisez `&`, `|`, `~`, pas `and`, `or`, `not`** dans les filtres, et parenthésez chaque
  condition. `and` sur des colonnes lève `Cannot convert column into bool`.
- **Lisez la Spark UI au fil de l'eau**, pas à la fin. C'est en regardant un job tourner que vous
  comprenez où est le temps. Repérez les Exchange (shuffle) dans le DAG.
- **Justifiez chaque optimisation.** Une optimisation ajoutée sans mesure ni explication ne compte
  pas. Mesurez un temps avant, appliquez, mesurez après, ou lisez le plan avec `explain()`.
- **Gardez du temps pour la restitution.** Arrêtez le code à 16h30 au plus tard. Une démo qui tourne
  et une explication claire valent mieux qu'un code ambitieux qui plante devant le groupe.
