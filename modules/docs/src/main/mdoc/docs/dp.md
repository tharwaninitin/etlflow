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

val dpConfig1 = DATAPROC(
              "@DP_PROJECT_ID@",
              "@DP_REGION@",
              "@DP_ENDPOINT@",
              "@DP_CLUSTER_NAME@"
            )
            
val step1 = DPHiveJobStep(
              name = "DPHiveJobStepExample",
              query = "SELECT 1 AS ONE",
              config = dpConfig1,
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
        config      = dpConfig1,
        main_class  = "@DP_MAIN_CLASS@",
        libs        = libs
) 
```

### DPCreateStep
We can use below step when we want to create new dataproc cluster.

```scala mdoc
import etlflow.etlsteps.DPCreateStep
import etlflow.gcp.DataprocProperties
import etlflow.utils.Executor.DATAPROC


   val dpConfig2 = DATAPROC(
          sys.env("DP_PROJECT_ID"),
          sys.env("DP_REGION"),
          sys.env("DP_ENDPOINT"),
          sys.env("DP_CLUSTER_NAME")
   )

   val dpProps =  DataprocProperties(
          bucket_name     = sys.env("DP_BUCKET_NAME"),
          subnet_uri      = sys.env.get("DP_SUBNET_WORK_URI"),
          network_tags    = sys.env("DP_NETWORK_TAGS").split(",").toList,
          service_account = sys.env.get("DP_SERVICE_ACCOUNT")
   )


val step3 = DPCreateStep(
          name     = "DPCreateStepExample",
          config   = dpConfig2,
          props    = dpProps
        )
```

### DPDeleteStep
We can use below step when we want to delete dataproc cluster.


```scala mdoc
import etlflow.utils.Executor.DATAPROC
import etlflow.etlsteps.DPDeleteStep

 val dpConfig3 = DATAPROC(
      sys.env("DP_PROJECT_ID"),
      sys.env("DP_REGION"),
      sys.env("DP_ENDPOINT"),
      sys.env("DP_CLUSTER_NAME")
 )

 val step = DPDeleteStep(
          name     = "DPDeleteStepExample",
          config   = dpConfig3,
 )

```