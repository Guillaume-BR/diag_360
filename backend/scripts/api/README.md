# Scripts API

Ce dossier accueillera les scripts qui appellent des API externes pour alimenter la table `valeur_indicateur`.

## Structure attendue

- Chaque script Python doit récupérer les données (ex: via `requests`),
  - transformer les réponses JSON en lignes `id_epci`, `id_indicateur`, `valeur_brute`, `annee`.
  - insérer/mettre à jour les enregistrements via SQLAlchemy ou des requêtes SQL.
- Utiliser les variables d'environnement (URL d'API, token...) au lieu de valeurs en dur.

## Exemple minimal

```bash
python fetch_indicateurs_eau.py --annee 2025
```

Libre à toi de créer un script par fournisseur d'API (DGCL, INSEE, etc.).
