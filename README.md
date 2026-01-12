# Diag360

Plateforme complète pour le diagnostic 360° de la résilience territoriale :

- **Backend** FastAPI + SQLAlchemy pour exposer les territoires, ingérer les référentiels Excel et calculer les scores.
- **Frontend** React/Vite (shadcn-ui) consommant l’API (`VITE_API_BASE_URL`).
- **Infra Docker** (Postgres, backend, frontend, NocoDB) pilotée par `docker-compose` et un script d’orchestration.

## Architecture rapide

```
Frontend (Vite/React)  <--->  FastAPI  <--->  PostgreSQL (tables FR : epci / indicateur / valeur_indicateur / score_indicateur)
                                           \
                                            -> NocoDB (admin UI)
```

- Le schéma PostgreSQL est défini dans `docker/postgres/init/002_create_diag360_schema.sql` (tables françaises + vues utilitaires).
- Scripts CLI : `ingest_workbook.py` (ingestion du classeur Excel), scripts d’API & de scores dans `backend/scripts/`.
- Outils : `scripts/run_pipeline.sh` pour orchestrer `docker compose`.

Voir `docs/architecture.md` pour une vue détaillée et `docs/caddy.md` pour l’exposition via Caddy.

## Démarrage rapide

```bash
cp .env.example .env           # compléter Postgres/NocoDB/API
./scripts/run_pipeline.sh up --build
```

### Ingestion XLSX + calcul

```bash
./scripts/run_pipeline.sh ingest /chemin/Diag360_EvolV2.xlsx
./scripts/run_pipeline.sh need-scores 0
```

### Frontend local

```bash
cd front
npm install
npm run dev
```

### Backend local

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Déploiement VPS

1. Copier le dépôt.
2. Configurer `.env`.
3. `./scripts/run_pipeline.sh up --build`.
4. Configurer Caddy (`docs/caddy.md`) pour exposer `serv1.diag360.org` et `nocodb.diag360.org`.
5. Suivre les étapes de `docs/deploy.md` (pipeline complet et exemples `fetch`).

## Scripts utiles

| Commande | Description |
|----------|-------------|
| `./scripts/run_pipeline.sh up --build` | Build & start stack Docker |
| `./scripts/run_pipeline.sh ingest [fichier]` | Charge un classeur Excel |
| `./scripts/run_pipeline.sh need-scores [année]` | (à adapter selon les scripts de calcul) |
| `python backend/scripts/seed_fake_scores.py --year 2025` | Injecte des scores fictifs pour tester le front |
| `./scripts/run_pipeline.sh logs` | Logs backend/front/nocodb |

## Tests & lint

```bash
cd front && npm run lint
cd backend && pytest  # (ajouter les tests souhaités)
```

## Licence

Projet sous licence MIT (voir `LICENSE`). Contributions bienvenues via PR.
