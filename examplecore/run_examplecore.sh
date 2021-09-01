sbt ";project examplecore; docker:publishLocal"

cd examplecore || exit

docker-compose up