---
layout: docs
title: Dataproc
---

## Dataproc Steps

**This page shows different Dataproc Steps available in this library**

### DPHiveJobStep
We can use below step when we want to trigger query on Hive Dataproc. Query should not return results for this step 

```scala mdoc
import etlflow.utils.Executor.DATAPROC
import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}

val dpConfig = DATAPROC(
              "@DP_PROJECT_ID@",
              "@DP_REGION@",
              "@DP_ENDPOINT@",
              "@DP_CLUSTER_NAME@"
            )
            
val step1 = DPHiveJobStep(
              name = "DPHiveJobStepExample",
              query = "SELECT 1 AS ONE",
              config = dpConfig,
)
```


### DPSparkJobStep
We can use below step when we want to trigger a job on Dataproc cluster from local server.

```scala mdoc
import etlflow.utils.Executor.DATAPROC
import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}

val libs = "@DP_LIBS@".split(",").toList
val step2 = DPSparkJobStep(
        name        = "DPSparkJobStepExample",
        job_name    = "@DP_JOB_NAME@",
        props       = Map.empty,
        config      = dpConfig,
        main_class  = "@DP_MAIN_CLASS@",
        libs        = libs
) 
```