#!/usr/bin/env bash

docker system prune

docker rmi -f $(docker images -f "dangling=true" -q)

# Below scripts can be used without setting docker main class in build.sbt
# Using --entrypoint /opt/docker/bin/...

docker run -it --rm --entrypoint /bin/bash etlflow:0.8.0

docker run -it --rm \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etlflow \
-e LOG_DB_USER=<localuser> \
-e LOG_DB_PWD= \
-e LOG_DB_DRIVER=org.postgresql.Driver \
--entrypoint /opt/docker/bin/load-data etlflow:0.8.0 show_job_props --default_values --job_name Job0DataprocPARQUETtoORCtoBQ

docker run -it --rm \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etlflow \
-e LOG_DB_USER=<localuser> \
-e LOG_DB_PWD= \
-e LOG_DB_DRIVER=org.postgresql.Driver \
--entrypoint /opt/docker/bin/load-data etlflow:0.8.0 show_job_props --actual_values --job_name Job0DataprocPARQUETtoORCtoBQ

docker run -it --rm \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etlflow \
-e LOG_DB_USER=<localuser> \
-e LOG_DB_PWD= \
-e LOG_DB_DRIVER=org.postgresql.Driver \
--entrypoint /opt/docker/bin/load-data etlflow:0.8.0 show_step_props --job_name Job0DataprocPARQUETtoORCtoBQ --props ratings_input_path=xyz

docker run -it --rm \
-e GOOGLE_APPLICATION_CREDENTIALS=/opt/docker/key/sa.json \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etlflow \
-e LOG_DB_USER=<localuser> \
-e LOG_DB_PWD= \
-e LOG_DB_DRIVER=org.postgresql.Driver \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
--entrypoint /opt/docker/bin/load-data etlflow:0.8.0 show_job_props --actual_values --job_name Job0DataprocPARQUETtoORCtoBQ

# To run server
docker run -it --rm \
-e GOOGLE_APPLICATION_CREDENTIALS=/opt/docker/key/sa.json \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etlflow \
-e LOG_DB_USER=<localuser> \
-e LOG_DB_PWD= \
-e LOG_DB_DRIVER=org.postgresql.Driver \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
etlflow:0.8.0