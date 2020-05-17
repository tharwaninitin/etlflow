#!/usr/bin/env bash

docker system prune
docker rmi -f $(docker images -f "dangling=true" -q)

docker run -it --rm --entrypoint /bin/bash etljobs:0.7.6

docker run -it --rm \
etljobs:0.7.6 show_job_props --default_values --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps

docker run -it --rm \
etljobs:0.7.6 show_job_props --actual_values --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps

docker run -it --rm \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e GCP_PROJECT=<gcsproject> \
etljobs:0.7.6 show_step_props --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps --props ratings_input_path=xyz

docker run -it --rm \
-e GCP_PROJECT_KEY_NAME=/opt/docker/key/sa.json \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e GCP_PROJECT=<gcsproject> \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etljobs \
-e LOG_DB_USER=<localuser> \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
etljobs:0.7.6 run_job --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps

docker run -it --rm \
-e GCP_PROJECT_KEY_NAME=/opt/docker/key/sa.json \
-e GOOGLE_APPLICATION_CREDENTIALS=/opt/docker/key/sa.json \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e GCP_PROJECT=<gcsproject> \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etljobs \
-e LOG_DB_USER=<localuser> \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
etljobs:0.7.6 run_job --job_name EtlJob5PARQUETtoJDBC