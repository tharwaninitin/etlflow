---
layout: docs
title: Dataproc
---

## Dataproc Steps

**This page shows different Dataproc Steps available in this library**

### DPHiveJobStep
We can use below step when we want to trigger query on Hive Dataproc. Query should not return results for this step 

    val dpConfig = DATAPROC(
              sys.env("DP_PROJECT_ID"),
              sys.env("DP_REGION"),
              sys.env("DP_ENDPOINT"),
              sys.env("DP_CLUSTER_NAME")
            )
            
    val step = DPHiveJobStep(
              name = "DPHiveJobStepExample",
              query = "SELECT 1 AS ONE",
              config = dpConfig,
            )

### DPSparkJobStep
We can use below step when we want to trigger a job on Dataproc cluster from local server.

    val dpConfig = DATAPROC(
              sys.env("DP_PROJECT_ID"),
              sys.env("DP_REGION"),
              sys.env("DP_ENDPOINT"),
              sys.env("DP_CLUSTER_NAME")
            )
            
    val step = DPSparkJobStep(
        name        = "DPSparkJobStepExample",
        job_name    = sys.env("DP_JOB_NAME"),
        props       = Map.empty,
        config      = dpConfig,
        main_class  = sys.env("DP_MAIN_CLASS"),
        libs        = libs
      ) 