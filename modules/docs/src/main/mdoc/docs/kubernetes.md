---
layout: docs
title: Kubernates Executor
---

## Kubernetes Executor

**When we provides the job execution as Kubernates then job will get's submitted on the Kubernates cluster with provided configurations**

## Parameters
* **args** [EtlJobArgs] - Job Details.
* **transactor** [HikariTransactor[Task]] - Database connection details. 
* **config** [KUBERNETES] - KUBERNETES connection details.

### Example 1
Below is the example to tell library about job should get executed on dataproc cluster. 

```scala mdoc

import etlflow.utils.Executor.KUBERNETES
import etlflow.utils.{Executor}

val kubernetes = KUBERNETES(
           "etlflow:0.7.19",
           "default",
           Map(
             "GOOGLE_APPLICATION_CREDENTIALS"-> Option("<cred_file>"),
             "LOG_DB_URL"-> Option("jdbc:postgresql://host.docker.internal:5432/postgres"),
             "LOG_DB_USER"-> Option("<username>"),
             "LOG_DB_PWD"-> Option("<pwd>"),
             "LOG_DB_DRIVER"-> Option("org.postgresql.Driver")
           )
         )
       
case class EtlJob5Props (
                     ratings_input_path: List[String] = List(""),
                     job_schedule: String = "0 0 5,6 ? * *",
                     ratings_output_table: String = "",
                     job_deploy_mode: Executor = kubernetes
)
```                     
In above case class we define **job_deploy_mode** as kubernetes which tells library on how this particular jobs will gets executed.             
     
          
          