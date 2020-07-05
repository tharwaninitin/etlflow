---
layout: docs
title: Scheduler Installation
---

## How to build docker image

* Go inside repo root folder and copy GCS credential file at location : **examples/src/main/conf**

* Update the file **examples/src/main/conf/loaddata.properties** with below content : 

        
        gcp_project = <PROJECT-NAME>
        running_environment = local
        gcs_output_bucket = <BUCKET-NAME>
        log_db_url=<log_db_url>
        log_db_user=<log_db_user>
        log_db_pwd=<log_db_pwd>
        log_db_driver=org.postgresql.Driver
        
* To build docker image we have to run below commands. Clone this git repo and go inside repo root folder and perform below commands: 

         
         > sbt
         > project examples
         > docker:publish

* Now we have successfully built an image with tag. Let's run this image with docker compose file. 

Docker compose file file contains the two services : 
* Postgres Service : Required to log information in postgres db.
* Etljobs Docker Image : Contains the runnable etl jobs.

* Now once image gets build in above steps. Lets create the docker-compose file to run docker image in local env. Below is the docker-compose.yml file : 
      
      
       version: '3.4'
       services:
         postgres:
          image: 'postgres:9.6'
          deploy:
            resources:
              limits:
                memory: 512M
          environment:
            - POSTGRES_USER     =etlflow
            - POSTGRES_PASSWORD =etlflow
            - POSTGRES_DB       =etlflow
          ports:
            - '5432:5432'
       job:
          image: 'etlflow'
          restart: always
          depends_on:
            - postgres
          environment:
            PROPERTIES_FILE_PATH: /opt/docker/conf/loaddata.properties
            GOOGLE_APPLICATION_CREDENTIALS: /opt/docker/conf/cred.json
            LOG_DB_URL : 'jdbc:postgresql://postgres:5432/etlflow'
            LOG_DB_USER: etlflow
            LOG_DB_PWD : etlflow
          ports:
            - '8080:8080'


* Once Above file gets created then run below command to start the services and webserver at http://localhost:8080/ : 
  
      
      docker-compose up -d (To start the service in background)
      docker-compose down  (To stop the service)
      docker logs etlflow_job_1 (To check the deployed container logs)
 
## Note
      
* To build an docker image build we have to include below settings in sbt  : 

      
      packageName in Docker := "etlflow",
      mainClass in Compile := Some("examples.RunCustomServer"),
      dockerRepository := "",
      dockerBaseImage := "",
      dockerExposedPorts ++= Seq(8080,8080),
      mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "loaddata.properties", "conf/loaddata.properties"),
      mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "cred.json", "conf/cred.json")
      
Here you can see we are copying the loaddata.properties file and cred.json file along with docker image.    
 
* we can also automate docker publish run using cloud build yaml file which will take care of publishing the image.