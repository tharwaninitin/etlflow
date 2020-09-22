#!/usr/bin/env bash

docker system prune
docker rmi -f $(docker images -f "dangling=true" -q)

docker run -it --rm --entrypoint /bin/bash etlflow:0.7.7

docker run -it --rm \
-e PROPERTIES_FILE_PATH=/opt/docker/conf/loaddata.properties \
etlflow:0.7.7 show_job_props --default_values --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps

docker run -it --rm \
-e PROPERTIES_FILE_PATH=/opt/docker/conf/loaddata.properties \
etlflow:0.7.7 show_job_props --actual_values --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps

docker run -it --rm \
-e PROPERTIES_FILE_PATH=/opt/docker/conf/loaddata.properties \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e GCP_PROJECT=<gcsproject> \
etlflow:0.7.7 show_step_props --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps --props ratings_input_path=xyz

docker run -it --rm \
-e PROPERTIES_FILE_PATH=/opt/docker/conf/loaddata.properties \
-e GCP_PROJECT_KEY_NAME=/opt/docker/key/sa.json \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e GCP_PROJECT=<gcsproject> \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etljobs \
-e LOG_DB_USER=<localuser> \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
etlflow:0.7.7 run_job --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps

docker run -it --rm \
-e PROPERTIES_FILE_PATH=/opt/docker/conf/loaddata.properties \
-e GCP_PROJECT_KEY_NAME=/opt/docker/key/sa.json \
-e GOOGLE_APPLICATION_CREDENTIALS=/opt/docker/key/sa.json \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e GCP_PROJECT=<gcsproject> \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etljobs \
-e LOG_DB_USER=<localuser> \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
etlflow:0.7.7 run_job --job_name EtlJob5PARQUETtoJDBC

# Below scripts can be used without setting docker main class in build.sbt
# Using --entrypoint /opt/docker/bin/...

docker run -it --rm \
-e PROPERTIES_FILE_PATH=/opt/docker/conf/loaddata.properties \
-e GCP_PROJECT_KEY_NAME=/opt/docker/key/sa.json \
-e GOOGLE_APPLICATION_CREDENTIALS=/opt/docker/key/sa.json \
-e GCP_REGION=<gcsregion> \
-e GCP_PROJECT=<gcsproject> \
-e GCP_DP_ENDPOINT=<gcsdpendpoint> \
-e GCP_DP_CLUSTER_NAME=<gcsdpcluster> \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etljobs \
-e LOG_DB_USER=<localuser> \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
--entrypoint /opt/docker/bin/load-data etlflow:0.7.7 --run_remote_job --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps

docker run -it --rm \
-e PROPERTIES_FILE_PATH=/opt/docker/conf/loaddata.properties \
-e GOOGLE_APPLICATION_CREDENTIALS=/opt/docker/key/sa.json \
-e GCP_PROJECT_KEY_NAME=/opt/docker/key/sa.json \
-e GCP_PROJECT=<gcsproject> \
-e GCS_OUTPUT_BUCKET=<gcsbucket> \
-e LOG_DB_URL=jdbc:postgresql://host.docker.internal:5432/etljobs \
-e LOG_DB_USER=<localuser> \
-v /Users/<localuser>/Desktop/pem/sa.json:/opt/docker/key/sa.json \
--entrypoint /opt/docker/bin/run-server etlflow:0.7.7