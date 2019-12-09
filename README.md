# ETL Pipeline

## Getting Started
1. To get a copy of this project use below git clone command and download a copy
on your local machine:
git clone https://github.com/tharwaninitin/etljobs.git

### Prerequisites
1. Install scala plugin for modifying existing code.
2. GCS authentication: Get service account google credentials json file and place it under etljobs folder with name gcsCred.json

## Running the tests
Automated test suites are created for each job covering all the functionality.
Use below command on command line to run all the test cases at once:
sbt test

## Built With
* [Spark](https://spark.apache.org/) - The in-memory computation framework used
* [Scala](https://www.scala-lang.org/) - Language used with spark jobs
* [Sbt](https://www.scala-sbt.org/download.html) - Dependency Management
* [GCP](https://cloud.google.com/) - Used to store batch data in many forms

