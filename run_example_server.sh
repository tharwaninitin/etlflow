sbt ";project examples; docker:publishLocal"

cd deployment/docker || exit

docker-compose run webserver run_db_migration

docker-compose run webserver add_user --user admin --password admin

docker-compose up