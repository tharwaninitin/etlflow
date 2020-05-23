#!/usr/bin/env bash
# Publish to local
sbt "project core" clean publishLocal

# Make sure GITHUB_TOKEN is properly set
echo $GITHUB_TOKEN

# https://github.com/tcnksm/ghr
# Uploads jar to Github releases page
ghr v0.7.9 modules/core/target/scala-2.12/etlflow-core_2.12-0.7.9.jar