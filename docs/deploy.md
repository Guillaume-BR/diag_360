# Déploiement & pipeline

## Prérequis

1. Copier le dépôt sur le VPS (`/srv/docker/shared/diag360` par exemple).
2. Renseigner `.env` (copie depuis `.env.example`) avec les secrets Postgres, NocoDB, etc.
3. Vérifier que Docker et le plugin Compose sont installés.

## Commandes principales

```bash
# Démarrer la stack
docker compose up -d db backend frontend nocodb

# Recréer complètement Postgres (dropping data !)
docker compose down
sudo rm -rf docker-data/postgres
docker compose up -d db

# Ingestion Excel
docker compose run --rm backend_ingest --file /data/Diag360_EvolV2.xlsx

# Seeder des scores fictifs
python backend/scripts/seed_fake_scores.py --year 2025
```

> `scripts/run_pipeline.sh` reste disponible mais il est recommandé d’utiliser directement les commandes ci-dessus pour plus de lisibilité.

## Reverse proxy (Caddy)

Voir `docs/caddy.md` pour les blocs `serv1.diag360.org` (frontend) et
`nocodb.diag360.org`. Après toute modification :

```bash
docker exec caddy caddy reload --config /etc/caddy/Caddyfile
```

## Séquence type

1. `docker compose up -d`
2. `docker compose run --rm backend_ingest --file /data/Diag360_EvolV2.xlsx`
3. (Optionnel) lancer les scripts `backend/scripts/api/…` puis `backend/scripts/scores/…`
4. Ajouter des scores de test (`python backend/scripts/seed_fake_scores.py --year 2025`)
5. Configurer/valider les routes Caddy.
6. Vérifier les services (`docker compose logs -f backend frontend nocodb`).
