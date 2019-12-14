## Follow below steps to generate documentation

## To generate Scala Test Coverage Report

Open sbt shell and run below command
```
;project etljobs; clean; coverage; test; coverageReport
```
exit the shell and copy generated docs
```
rm -rf docs/testcovrep
cp -r etljobs/target/scala-2.11/scoverage-report/ docs/testcovrep
```
## To generate Scala API Documentation

Open sbt shell and run below command
```
;project etljobs; doc
```
exit the shell and copy generated docs
```
rm -rf docs/api
cp -r etljobs/target/scala-2.11/api/ docs/api
```