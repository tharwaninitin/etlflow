sbt ";project exampleserver; docker:publishLocal"

cd exampleserver || exit

docker-compose run webserver initdb

docker-compose run webserver add_user --user admin --password admin

docker-compose up