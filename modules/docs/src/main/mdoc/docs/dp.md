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

### DPCreateStep
We can use below step when we want to create new dataproc cluster.

```scala mdoc
import etlflow.utils.Executor.DATAPROC
import etlflow.etlsteps.{DPCreateStep}

val create_cluster_spd_props: Map[String, String] =
          Map(
            "project_id" -> "DP_PROJECT_ID",
            "region" -> "DP_REGION",
            "endpoint" -> "DP_ENDPOINT",
            "bucket_name" -> "BUCKET_NAME",
            "image_version" -> "IMAGE_VERSION",
            "boot_disk_type" -> "BOOT_DISK_TYPE",
            "master_boot_disk_size" -> "MASTER_BOOT_DISK_SIZE",
            "worker_boot_disk_size" -> "WORKER_BOOT_DISK_SIZE",
            "subnet_work_uri" -> "SUBNET_WORK_URI",
            "all_tags" -> "ALL_TAGS",
            "master_machine_type_uri" -> "MASTER_MACHINE_TYPE_URI",
            "worker_machine_type_uri" -> "WORKER_MACHINE_TYPE_URI",
            "master_num_instance" -> "MASTER_NUM_INSTANCE",
            "worker_num_instance" -> "WORKER_NUM_INSTANCE"
          )

val step3 = DPCreateStep(
          name                    = "DPCreateStepExample",
          cluster_name            = "test",
          props                   = create_cluster_spd_props
        )

```

### DPDeleteStep
We can use below step when we want to delete dataproc cluster.


```scala mdoc
import etlflow.utils.Executor.DATAPROC
import etlflow.etlsteps.{DPDeleteStep}


val delete_cluster_spd_props: Map[String, String] =
          Map(
            "project_id" -> "DP_PROJECT_ID",
            "region" -> "DP_REGION",
            "endpoint"-> "DP_ENDPOINT",
          )

val step4 = DPDeleteStep(
          name                    = "DPDeleteStepExample",
          cluster_name            = "test",
          props                   = delete_cluster_spd_props
        )

```