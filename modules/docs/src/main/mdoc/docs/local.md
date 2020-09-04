---
layout: docs
title: Local Executor
---

## Local Executor

**This default mode of job execution. When we provide the job execution as local then job will gets submitted on the local environment**

## Parameters
* **args** [EtlJobArgs] - Job Details.
* **transactor** [HikariTransactor[Task]] - Database connection details. 

### Example 1
Below is the example to tell library about job should get executed on dataproc cluster. 

```scala mdoc

import etlflow.utils.{Executor}
      
case class EtlJob5Props (
                     ratings_input_path: List[String] = List(""),
                     job_schedule: String = "0 0 5,6 ? * *",
                     ratings_output_table: String = "",
                     job_deploy_mode: Executor = Executor.LOCAL
)
```                     
In above case class we define **job_deploy_mode** as local which tells library on how this particular jobs will gets executed.             
          
          