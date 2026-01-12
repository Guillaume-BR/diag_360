# NocoDB — Diag360

Ce guide explique comment connecter et utiliser NocoDB avec la base PostgreSQL provisionnée par `docker-compose`.

## 1. Démarrage des services

```bash
docker compose up -d db backend frontend nocodb
```

La variable d'environnement `NC_DB` dans `docker-compose.yml` pointe déjà vers PostgreSQL (`diag360`/`diag360pwd`).

## 2. Connexion initiale

1. Ouvrir `http://localhost:8081` (ou l'URL publique configurée).
2. Se connecter avec les identifiants définis dans `.env` (`NOCODB_EMAIL`, `NOCODB_PASSWORD`).
3. À la première connexion, NocoDB détectera automatiquement la base `diag360`. Choisir **Use Existing** pour éviter de recréer une nouvelle connexion.

## 3. Organisation recommandée

Depuis la refonte, les tables principales vivent dans le schéma `public`. Pour simplifier :

| Vues/Tables | Contenu | Actions |
|-------------|---------|---------|
| `vue_indicateur_details` | Indicateurs + besoins/objectifs/types (relations via `indicateur_*`) | Lecture/édition des métadonnées |
| `epci` | Liste des EPCI (SIREN, label, stats) | Lecture/édition limitée |
| `valeur_indicateur` | Valeurs brutes (EPCI × indicateur × année) – alimentées par les scripts/API | Lecture + corrections ponctuelles |
| `score_indicateur` | Scores calculés par indicateur (besoin/objectifs/types) | Lecture seule (produits par les scripts de scoring) |
| `score_global` | Scores agrégés par EPCI (global + composantes) | Lecture seule (produits par les scripts de scoring global) |
| `territories` | Table existante pour le front historique | Lecture |

### Étapes dans NocoDB

1. Dans le workspace, cliquer sur **Create Project → Existing DB**.
2. Utiliser la connexion par défaut (`NC_DB`). Laisser le champ *Schema* vide pour accéder à `public`.
3. Ajouter les tables/vues (`epci`, `indicateur`, `valeur_indicateur`, `score_indicateur`, `vue_indicateur_details`).
4. Ajouter des relations (ex. `score_indicateur.id_epci → epci.id_epci`) pour faciliter les formulaires.

### Permissions

- Créer au moins 2 rôles :
  - **Admin** : accès complet aux schémas `diag360_ref` & `diag360_raw`.
  - **Lecture** : accès en lecture seule pour les partenaires.
- Utiliser les **Shared Views** pour publier un sous-ensemble de données sans exposer la totalité de la base.

## 4. Synchronisation avec les imports Excel

1. Exécuter l'ingestion (CLI ou futur script) pour alimenter `diag360_ref` et `diag360_raw`.
2. Rafraîchir NocoDB : les nouvelles lignes sont visibles instantanément.
3. Éviter de modifier manuellement les colonnes calculées (ex: `indicator_scores.score`). Privilégier les colonnes descriptives ou les tables de correspondance.

## 5. Points d'attention

- Changer `NOCODB_JWT_SECRET`, `NOCODB_EMAIL`, `NOCODB_PASSWORD` avant toute mise en ligne.
- Restreindre l'accès (VPN ou reverse proxy avec authentification) : NocoDB a des droits d'écriture sur toute la base.
- En production, monter un volume Docker pour `/usr/app/data` afin de conserver la configuration NocoDB (tables, vues, rôles).
