---
layout: docs
title: Dataproc
---

## Dataproc Steps

**This page shows different Dataproc Steps available in this library**

### DPHiveJobStep
We can use below step when we want to trigger query on Hive Dataproc. Query should not return results for this step 

```scala mdoc
import etlflow.schema.Executor.DATAPROC
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
import etlflow.schema.Executor.DATAPROC
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
import etlflow.schema.Executor.DATAPROC


   val dpConfig2 = DATAPROC(
              "@DP_PROJECT_ID@",
              "@DP_REGION@",
              "@DP_ENDPOINT@",
              "@DP_CLUSTER_NAME@"
            )

   val dpProps =  DataprocProperties(
          bucket_name     = "DP_BUCKET_NAME",
          subnet_uri      = Some("DP_SUBNET_WORK_URI"),
          network_tags    = "DP_NETWORK_TAG1,DP_NETWORK_TAG2".split(",").toList,
          service_account = Some("DP_SERVICE_ACCOUNT")
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
import etlflow.schema.Executor.DATAPROC
import etlflow.etlsteps.DPDeleteStep

  val dpConfig3 = DATAPROC(
              "@DP_PROJECT_ID@",
              "@DP_REGION@",
              "@DP_ENDPOINT@",
              "@DP_CLUSTER_NAME@"
            )

 val step = DPDeleteStep(
          name     = "DPDeleteStepExample",
          config   = dpConfig3,
 )

```