---
layout: docs
title: Redis Query 
---

## Redis Query Step

**This page shows use of redis query step available in this library**

## Parameters
* **name** [String] - Description of the Step.
* **command** [String] - Action to perform(RedisCmd.SET/RedisCmd.DELETE/RedisCmd.FLUSHALL). 
* **credentials** [REDIS] - Redis credentials

### Example 1

```scala mdoc
import etlflow.EtlStepList
import etlflow.etlsteps.{EtlStep, RedisStep}
import etlflow.Credential.REDIS
import etlflow.etlsteps.RedisStep.RedisCmd

      
val redis_config: REDIS = REDIS("localhost")
      
val step1 = RedisStep(
    name         = "Set redis key and value",
    command      = RedisCmd.SET(Map("key1" -> "value1","key2" -> "value3","key3" -> "value3")),
    credentials  = redis_config
  )

  val step2 = RedisStep(
    name        = "delete the keys from redis",
    command     = RedisCmd.DELETE(List("*key1*")),
    credentials = redis_config
  )

  val step3 = RedisStep(
    name        = "flushall the keys from redis",
    command     = RedisCmd.FLUSHALL,
    credentials = redis_config
  )
```