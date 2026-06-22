#!/usr/bin/env bash
# Telecharge les jeux de donnees du fil rouge du cours Spark.
# Usage : data/download.sh [dossier_cible]
# Par defaut, ecrit dans data/datasets/.
set -euo pipefail

target="${1:-data/datasets}"
mkdir -p "$target"

cdn="https://d37ci6vzurychx.cloudfront.net"

echo "== NYC Yellow Taxi (fil rouge principal) =="
for ym in 2024-01 2024-02 2024-03; do
  out="$target/yellow_tripdata_${ym}.parquet"
  if [ -f "$out" ]; then
    echo "  deja present : $out"
  else
    echo "  telechargement : $out"
    curl -fSL "$cdn/trip-data/yellow_tripdata_${ym}.parquet" -o "$out"
  fi
done

echo "== Table des zones taxi (pour les jointures) =="
zones="$target/taxi_zone_lookup.csv"
if [ -f "$zones" ]; then
  echo "  deja present : $zones"
else
  curl -fSL "$cdn/misc/taxi_zone_lookup.csv" -o "$zones"
fi

echo "== DVF un departement (exemple : 75 Paris, annee 2023) =="
dvf="$target/dvf_75_2023.csv.gz"
if [ -f "$dvf" ]; then
  echo "  deja present : $dvf"
else
  curl -fSL "https://files.data.gouv.fr/geo-dvf/latest/csv/2023/departements/75.csv.gz" -o "$dvf" \
    || echo "  (echec DVF, a verifier manuellement sur data.gouv.fr)"
fi

echo "== MovieLens small (jointures et intro MLlib) =="
ml="$target/ml-latest-small.zip"
if [ -f "$ml" ]; then
  echo "  deja present : $ml"
else
  curl -fSL "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip" -o "$ml" \
    && (cd "$target" && unzip -o ml-latest-small.zip >/dev/null) \
    || echo "  (echec MovieLens, a verifier manuellement)"
fi

echo ""
echo "Termine. Contenu de $target :"
ls -lh "$target"
echo ""
echo "Astuce : ces fichiers sont volumineux, ne pas les committer dans Git."
