# luigi-ml-service
A ML experiment system implemented by luigi, redis, postgres and docker.

# Quick Start

## Build with docker

```bash
docker-compose -f docker-compose-dev.yml
```
## Modify model_config.yaml

## Run the modeling experiment

```bash
docker exec -it luigi_service python -m luigi --module pipeline.tasks Experiment --model-config=model_config.yaml --workers=4
```

## Open the dashboard at `localhost:9091` on the browser

## Use results schema for post-modeling anlaysis 
```bash
docker exec -it luigi_db psql -d luigid -U luigid
```
