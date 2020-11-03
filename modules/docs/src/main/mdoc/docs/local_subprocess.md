---
layout: docs
title: Local Sub-Process Executor
---

## Local Sub-Process Executor

**When we provides the job execution as local sub process then job will gets submit on another jvm as local process**

## Parameters
* **args** [EtlJobArgs] - Job Details.
* **transactor** [HikariTransactor[Task]] - Database connection details. 

### Example 1
Below is the example to tell library about job should get executed on local subprocess mode.

```scala mdoc

import etlflow.utils.{Executor}
import etlflow.utils.Executor.LOCAL_SUBPROCESS

val local_subprocess = LOCAL_SUBPROCESS("examples/target/docker/stage/opt/docker/bin/load-data",heap_min_memory = "-Xms100m",heap_max_memory = "-Xms100m")

case class EtlJob5Props (
                     ratings_input_path: List[String] = List(""),
                     job_schedule: String = "0 0 5,6 ? * *",
                     ratings_output_table: String = "",
                     job_deploy_mode: Executor = local_subprocess
)
```                     
In above case class we define **job_deploy_mode** as local subprocess which tells library on how this particular jobs will gets executed.             
          
          