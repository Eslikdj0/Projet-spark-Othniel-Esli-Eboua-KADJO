# Sources open data pour le cours Apache Spark

Inventaire des jeux de données ouverts utilisés dans la formation, pour illustrer Spark
à la fois comme outil d'ingestion et comme moteur de calcul distribué.

Statut de vérification : les sources marquées "Vérifié le 2026-06-21" ont été confirmées
en ligne le jour de la rédaction (recherche web). Les autres reposent sur des patterns
d'URL stables et bien connus, à reconfirmer avec le script `data/download.sh` avant la session.

Convention : pas de clé d'API requise sauf mention contraire. Toujours vérifier la licence
avant un usage autre que pédagogique.

---

## 1. Fil rouge principal : NYC TLC Trip Record Data (taxis de New York)

C'est le jeu de données de référence pour enseigner Spark. Format Parquet natif, partitionné
par mois, colonnes riches, volume réglable (un mois tient sur un laptop, plusieurs mois
permettent de sentir le parallélisme).

- Page officielle : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (Vérifié le 2026-06-21)
- Registre AWS Open Data : https://registry.opendata.aws/nyc-tlc-trip-records-pds/ (Vérifié le 2026-06-21)
- Pattern de téléchargement direct (CDN CloudFront) :
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_AAAA-MM.parquet`
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_AAAA-MM.parquet`
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_AAAA-MM.parquet`
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_AAAA-MM.parquet`
  - Exemple concret : `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`
- Table de correspondance des zones (pour les jointures) :
  - `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Format : Parquet (depuis 2022 ; CSV pour les années plus anciennes)
- Volume : environ 3 millions de lignes par mois pour le Yellow taxi, ~50 Mo compressés par fichier mensuel
- Granularité : un fichier Parquet par mois et par type de service
- Licence : données publiques de la ville de New York (usage libre)
- Note 2025+ : une colonne `cbd_congestion_fee` a été ajoutée (péage de congestion) (Vérifié le 2026-06-21)

Colonnes clés : `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`,
`trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`,
`total_amount`, `payment_type`.

Intérêt pédagogique :
- Parquet natif : parfait pour parler de colonnaire, schéma embarqué, predicate pushdown
- Partitionnement mensuel : lecture de plusieurs fichiers, lecture partielle, partition pruning
- Colonnes temporelles et géographiques : `groupBy`, agrégations, window functions, jointures
- Volume réglable : 1 mois pour débuter, 12 mois pour montrer le shuffle et le cache

Idées d'analyses : revenu par heure de la journée, pourboire moyen par zone, durée moyenne
des courses par jour de semaine, top 10 des trajets zone à zone, détection d'anomalies de tarif.

---

## 2. DVF : Demandes de valeurs foncières géolocalisées (immobilier France)

Toutes les transactions immobilières en France sur cinq ans. Excellent pour un public francophone.

- Page data.gouv.fr : https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees (Vérifié le 2026-06-21)
- Fichiers Etalab geo-dvf (pattern) :
  - `https://files.data.gouv.fr/geo-dvf/latest/csv/AAAA/full.csv.gz` (toute la France pour une année)
  - `https://files.data.gouv.fr/geo-dvf/latest/csv/AAAA/departements/DD.csv.gz` (par département)
  - `https://files.data.gouv.fr/geo-dvf/latest/csv/AAAA/communes/DD/CCCCC.csv.gz` (par commune)
- Format : CSV compressé gzip ; mise à jour régulière (dernière maj 2026)
- Volume : version France entière ~499 Mo compressé ; un département est beaucoup plus léger (idéal TP)
- Licence : Licence Ouverte / Open License (Etalab)

Colonnes clés : `date_mutation`, `valeur_fonciere`, `code_postal`, `nom_commune`,
`code_departement`, `type_local`, `surface_reelle_bati`, `nombre_pieces_principales`,
`longitude`, `latitude`.

Intérêt pédagogique : gros CSV gzip (lecture, schéma explicite, nettoyage), prix au m2,
agrégations par commune/département, évolution temporelle, valeurs aberrantes à filtrer.
Conseil TP : commencer par un seul département (par exemple `75.csv.gz` ou `33.csv.gz`).

---

## 3. Accidents corporels de la circulation routière (ONISR / BAAC)

Tous les accidents corporels déclarés en France, un jeu par année, réparti en quatre fichiers
relationnels. Parfait pour enseigner les jointures multi-tables.

- Page data.gouv.fr : https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/ (Vérifié le 2026-06-21)
- Page open data ONISR : https://www.onisr.securite-routiere.gouv.fr/en/data-tools/open-data (Vérifié le 2026-06-21)
- Quatre fichiers CSV par année : `caracteristiques`, `lieux`, `vehicules`, `usagers`
- Format : CSV, séparateur point-virgule, encodage à vérifier (souvent latin1 sur les années anciennes)
- Volume : environ 55 000 à 60 000 accidents par an, plusieurs centaines de milliers de lignes cumulées
- Licence : Licence Ouverte / Open License

Intérêt pédagogique : quatre tables à joindre sur `Num_Acc`, donc démonstration idéale des
jointures et du shuffle ; colonnes catégorielles (gravité, météo, type de route) pour les
agrégations ; données temporelles et géographiques.

Idées d'analyses : gravité par condition météo, accidents par heure et jour, cartographie par
département, profils d'usagers, évolution annuelle.

---

## 4. Velib Métropole (mobilité temps réel, Paris)

Disponibilité des vélos et bornes en temps réel, au standard GBFS. Idéal pour parler
d'ingestion de flux JSON et d'une mini-démo de Structured Streaming (poll de l'API).

- Page open data : https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole (Vérifié le 2026-06-21)
- Jeux opendata.paris.fr :
  - https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/ (Vérifié le 2026-06-21)
  - https://opendata.paris.fr/explore/dataset/velib-emplacement-des-stations/ (Vérifié le 2026-06-21)
- Racine GBFS communément utilisée : `https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/gbfs.json` (à reconfirmer)
  - puis `station_information.json` (statique) et `station_status.json` (mis à jour chaque minute)
- Format : JSON (GBFS 1.0), sans clé d'API
- Licence : Licence Ouverte / Open License

Intérêt pédagogique : ingestion JSON, schéma imbriqué, jointure station_status + station_information,
boucle de collecte simple pour simuler un flux et alimenter une démo de streaming.

---

## 5. MovieLens (notes de films, GroupLens)

Le classique pour parler de jointures, d'agrégations et d'introduction à MLlib (recommandation ALS).

- Page : https://grouplens.org/datasets/movielens/ (Vérifié le 2026-06-21)
- Téléchargements :
  - Petit (TP rapide) : `https://files.grouplens.org/datasets/movielens/ml-latest-small.zip`
  - 25 millions de notes : `https://files.grouplens.org/datasets/movielens/ml-25m.zip`
- Format : CSV (ratings.csv, movies.csv, tags.csv, links.csv)
- Volume : ml-25m = 25 000 095 notes, 62 423 films, 162 541 utilisateurs
- Licence : usage recherche et éducation, citation demandée (voir README GroupLens)

Intérêt pédagogique : jointures ratings + movies, films les mieux notés, popularité, et
démonstration MLlib avec l'algorithme ALS de recommandation.

---

## 6. Sources complémentaires (volume ou variété)

À reconfirmer avec `data/download.sh` avant usage. Patterns d'URL stables et bien connus.

- Base SIRENE (entreprises, INSEE) : https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
  - Fichiers stock sur `https://files.data.gouv.fr/insee-sirene/` (StockUniteLegale, StockEtablissement), très gros CSV. Démonstration de scale.
- NOAA GHCN-Daily (météo mondiale) : https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/
  - Par année : `by_year/AAAA.csv.gz` ; également sur AWS `s3://noaa-ghcn-pds/`. Séries temporelles, gros volume.
- Open Food Facts (produits alimentaires) : https://world.openfoodfacts.org/data
  - Exports CSV / JSONL / Parquet sur `https://static.openfoodfacts.org/data/` ; également Hugging Face `openfoodfacts/product-database` (Parquet). Données textuelles riches, valeurs manquantes.
- OpenFlights (aéroports et routes) : https://openflights.org/data.html
  - `airports.dat`, `routes.dat`, `airlines.dat` (CSV). Petit, parfait pour les jointures et une approche graphe.
- Backblaze Hard Drive Stats : https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data
  - Archives trimestrielles. Séries temporelles SMART, calcul de taux de panne.
- GDELT Project : http://data.gdeltproject.org/ (master file list). Très gros volume événementiel mondial.
- Common Crawl : https://commoncrawl.org/ (`s3://commoncrawl/`). Échelle pétaoctet, à citer pour parler de vrai Big Data sans tout exécuter.

---

## Classement par usage dans le cours

### Fil rouge et TP guidés (taille moyenne, propre, téléchargeable sur laptop)
- NYC Yellow Taxi (1 à 3 mois) + table des zones : fil rouge principal J1 à J3
- DVF un département : exercices d'ingestion CSV et nettoyage
- MovieLens small : jointures et intro MLlib

### Projet Jour 4 (assez riche pour un ETL + analyse complet)
- NYC Taxi multi-mois (6 à 12 mois) avec jointure zones
- Accidents ONISR (quatre tables à joindre)
- DVF France ou région (immobilier)
- MovieLens 25M (recommandation)

### Démonstration de scale (très gros, on n'exécute pas tout)
- SIRENE, NOAA GHCN, GDELT, Common Crawl

---

## Script de téléchargement

Voir `data/download.sh` pour récupérer les jeux du fil rouge en une commande.
Penser à vérifier l'espace disque et la connexion avant la session.
