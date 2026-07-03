# Projet Jour 4 — Option A : ONISR Accidents corporels

Ce dossier contient la structure de dépôt pour la soumission du projet Jour 4.

## Choix du jeu de données
- Option A : ONISR Personal Injury Accidents (sécurité routière France)
- Quatre tables relationnelles liées par `Num_Acc` : `caracteristiques`, `lieux`, `vehicules`, `usagers`
- Analyse attendue sur les jointures multi-tables, agrégations, window functions et optimisation.

## Contenu du dossier
- `rapport-jour-4-onsir.md` : rapport de projet avec toutes les sections attendues et des espaces réservés pour les résultats.

## Mode d'emploi
1. Copier ou adapter `starter-code/pipeline.py` pour lire les fichiers ONISR et produire :
   - couche silver propre en Parquet
   - résultats gold pour les analyses
2. Vérifier que les fichiers ONISR sont placés dans `data/datasets/onisr/` :
   - `caracteristiques_2023.csv`
   - `lieux_2023.csv`
   - `vehicules_2023.csv`
   - `usagers_2023.csv`
3. Exécuter le pipeline ONISR depuis la racine du projet :
   ```bash
   python projects/projet-jour-4-onsir/pipeline_onsir.py
   ```
4. Noter les métriques de temps et les observations de la Spark UI.
5. Compléter `rapport-jour-4-onsir.md` avec les extraits de code, résultats et captures.

## Notes
- Le livrable principal est le rapport écrit.
- Les résultats obtenus après l'exécution du pipeline doivent être insérés dans le rapport.
- Les fichiers Parquet silver et les résultats agrégés devront être ajoutés au dépôt après génération.
