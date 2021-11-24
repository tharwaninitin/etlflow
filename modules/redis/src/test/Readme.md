#Set up redis instance
```shell
docker run -it --rm -p 6379:6379 redis
```

#Run Tests
```shell
sbt ";project redis; +test"
```
