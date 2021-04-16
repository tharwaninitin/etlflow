#!/usr/bin/env bash
git tag 0.10.0

git push origin master --tags

# Publish to local
sbt "project root" clean publishLocal

# Make sure GITHUB_TOKEN is properly set
echo $GITHUB_TOKEN

# https://github.com/tcnksm/ghr
# Uploads jar to Github releases page
ghr 0.10.0 modules/core/target/scala-2.12/etlflow-core_2.12-0.10.0.jar
ghr 0.10.0 modules/server/target/scala-2.12/etlflow-server_2.12-0.10.0.jar
ghr 0.10.0 modules/cloud/target/scala-2.12/etlflow-cloud_2.12-0.9.10.jar
ghr 0.10.0 modules/spark/target/scala-2.12/etlflow-spark_2.12-0.9.10.jar