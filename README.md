# luigi-ml-service
A data/model agnostic ML experiment system implemented by luigi, redis, postgres and docker.
This project was spinned off from a talk I gave virtually at Instituto Tecnológico Autónomo de México of course over Zoom. The slide of the talk and the part of this tool is [here](http://tweddielin.com/slides/data_science_engineering_view/#35)

# Quick Start

#### Copy luigi.cfg.example to luigi.cfg

```bash
cp luigi.cfg.example luigi.cfg
```

#### Build with docker

```bash
docker-compose -f docker-compose-dev.yml
```

#### Modify model_config.yaml

#### Run the modeling experiment

```bash
docker exec -it luigi_service python -m luigi --module pipeline.tasks Experiment --model-config=model_config.yaml --workers=4
```

#### Open the dashboard by typing `localhost:9091` on the browser

#### Use results schema for post-modeling anlaysis 
```bash
docker exec -it luigi_db psql -d luigid -U luigid
```

# Run Locally

#### Start Postgres Database

#### Start Redis Service

#### Configure `luigi.cfg` with correct connection credential for Postgres and Redis

#### Run `luigid` luigi scheduler daemon
```bash
luigid --background --port=9091 --pidfile <PATH_TO_PIDFILE> --logdir <PATH_TO_LOGDIR> --state-path <PATH_TO_STATEFILE>
```

#### Open the dashboard by typing `localhost:9091` on the browser
