# bot-service-02

Fleet Swarm is a full-stack demo service for the RCA/rollback platform.

It intentionally looks like an infrastructure-heavy bot fleet:

- React/Vite web UI
- FastAPI backend
- PostgreSQL database
- Kubernetes manifests for API, web, DB, HPA, and LoadBalancer
- bounded incident scenarios for CPU burn, queue flooding, database stress, lock contention, error spikes, crash loops, and bad rollout behavior
- stable, bad-config, and rollback release overlays

The service is designed to be deployed to `cluster-2` and exposed as:

```text
https://bot-02.woonyong.org
```

## Kubernetes Deploy

The manifests do not commit real secret values. Create the DB secret at deploy time:

```bash
DB_PASSWORD='change-me' ./scripts/deploy.sh cluster-2 stable
```

Bad rollout:

```bash
DB_PASSWORD='change-me' ./scripts/deploy.sh cluster-2 bad-config
```

Rollback:

```bash
DB_PASSWORD='change-me' ./scripts/deploy.sh cluster-2 rollback
```

## Images

GitHub Actions builds:

```text
ghcr.io/jungle-303-04/bot-service-02-api:latest
ghcr.io/jungle-303-04/bot-service-02-web:latest
```

