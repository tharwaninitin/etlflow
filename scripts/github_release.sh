# Publish to local
sbt "project etljobs" clean publishLocal

# Make sure GITHUB_TOKEN is properly set
echo $GITHUB_TOKEN

# https://github.com/tcnksm/ghr
# Uploads jar to Github releases page
ghr v0.6.0 etljobs/target/scala-2.11/etljobs_2.11-0.6.0.jar