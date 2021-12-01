cd examplecore || exit

docker-compose up

sbt ";project examplecore; runMain etlflow.InitDB"