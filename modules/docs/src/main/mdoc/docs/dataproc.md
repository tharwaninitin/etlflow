---
layout: docs
title: Dataproc Executor
---

## Dataproc Executor

**When we provides the job execution as dataproc then job will gets submit on the gcs dataproc cluster with provided configurations**

## Parameters
* **args** [EtlJobArgs] - Job Details.
* **transactor** [HikariTransactor[Task]] - Database connection details. 
* **config** [DATAPROC] - DATAPROC connection details.

### Example 1
Below is the example to tell library about job should get executed on dataproc cluster. 


```scala mdoc

import etlflow.utils.Executor.DATAPROC
import etlflow.utils.{Executor}

val dataproc   = DATAPROC("project-name","region","endpoint","cluster-name")
       
case class EtlJob5Props (
      ratings_input_path: List[String] = List(""),
      job_schedule: String = "0 0 5,6 ? * *",
      ratings_output_table: String = "",
      job_deploy_mode: Executor = dataproc
)
```                     
In above case class we define **job_deploy_mode** as dataproc which tells library on how this particular jobs will gets executed.             
               