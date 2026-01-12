# Scripts de calcul des scores

Ce dossier est dédié aux scripts qui lisent les valeurs brutes (`valeur_indicateur`) et écrivent les résultats dans `score_indicateur`.

## Principes

1. **Entrée** : sélectionner les lignes pertinentes dans `valeur_indicateur` (par EPCI, indicateur, année) et appliquer les règles de calcul (normalisation, pondérations, etc.).
2. **Sortie** : insérer/mettre à jour `score_indicateur` en renseignant les colonnes :
   - `score_indicateur`
   - `score_besoin`, `score_objectif`, `score_type`, `score_global`
   - `rapport` (JSON facultatif pour stocker le détail du calcul)
3. **Organisation** : un script par famille de règles (ex. `calcul_eau.py`, `calcul_mobilite.py`).

## Exemple minimal

```bash
python calcul_scores_generiques.py --annee 2025
```

Le script ouvre une session SQLAlchemy, lit `valeur_indicateur`, appelle les fonctions de transformation puis écrit dans `score_indicateur`.
