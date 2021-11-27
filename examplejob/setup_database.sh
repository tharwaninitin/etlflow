cd examplejob || exit

docker-compose up

sbt ";project examplejob; runMain examples.LoadData initdb"