#!/usr/bin/env bash
git tag 0.7.19

git push origin master --tags

# Publish to local
sbt "project root" clean publishLocal

# Make sure GITHUB_TOKEN is properly set
echo $GITHUB_TOKEN

# https://github.com/tcnksm/ghr
# Uploads jar to Github releases page
ghr 0.7.19 modules/core/target/scala-2.12/etlflow-core_2.12-0.7.19.jar
ghr 0.7.19 modules/scheduler/target/scala-2.12/etlflow-scheduler_2.12-0.7.19.jar