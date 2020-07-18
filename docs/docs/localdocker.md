---
layout: docs
title: Scheduler Installation
---

## Using Etlflow scheduler library on local environment with Docker Compose

### Prerequisite
* Your system should have [Docker](https://docs.docker.com/get-docker/) installed.
* You should have GCP service account access with appropriate permissions for GCS, BigQuery, Submitting jobs on Dataproc etc.

### STEP 1) Building docker image for examples project.
* Update the file **examples/src/main/conf/loaddata.properties** with below content : 
        
        gcp_project = <PROJECT-NAME>
        running_environment = local
        gcs_output_bucket = <BUCKET-NAME>
        log_db_url=<log_db_url>
        log_db_user=<log_db_user>
        log_db_pwd=<log_db_pwd>
        log_db_driver=org.postgresql.Driver

* Copy GCP service account json file at location **examples/src/main/conf** with name **cred.json**

* To build docker image we have to run below commands. 

         
         > sbt
         > project examples
         > docker:publishLocal

### STEP 2) Now we have successfully built an image. Let's run this image with help of docker-compose file. 

Below Docker compose file file contains the two services : 
* postgres Service: Required to log information in postgres db.
* webserver Service: Scheduler and webserver for running etljobs based on cron.
      
      
      version: '3.4'
      services:
        postgres:
          image: 'postgres:9.6'
          deploy:
            resources:
              limits:
                memory: 512M
          environment:
          - POSTGRES_USER=etlflow
          - POSTGRES_PASSWORD=etlflow
          - POSTGRES_DB=etlflow
          ports:
          - '5432:5432'
        webserver:
          image: 'etlflow:0.7.18'
          deploy:
            resources:
              limits:
                memory: 2G
          restart: always
          depends_on:
          - postgres
          environment:
              PROPERTIES_FILE_PATH: /opt/docker/conf/loaddata.properties
              GOOGLE_APPLICATION_CREDENTIALS: /opt/docker/conf/cred.json
              LOG_DB_URL: 'jdbc:postgresql://postgres:5432/etlflow'
              LOG_DB_USER: etlflow
              LOG_DB_PWD: etlflow
              LOG_DB_DRIVER: org.postgresql.Driver
          ports:
          - '8080:8080'


* Use below commands to run/stop containers defined by docker-compose file: 
  
      
      docker-compose --compatibility config (To check config)
      docker-compose --compatibility up -d (To start the service in background)
      docker-compose --compatibility down  (To stop the service)
      docker logs etlflow_webserver_1 (To check the deployed container logs)
 
## Note
* Below are the settings for docker configuration in examples.sbt: 

      
      packageName in Docker := "etlflow",
      mainClass in Compile := Some("examples.RunCustomServer"),
      dockerBaseImage := "openjdk:jre",
      dockerExposedPorts ++= Seq(8080),
      mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "loaddata.properties", "conf/loaddata.properties"),
      mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "cred.json", "conf/cred.json")
      
* Here you can see we are copying the loaddata.properties file and cred.json file inside docker image.    