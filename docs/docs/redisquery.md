---
layout: docs
title: Redis Query 
---

## Redis Query Step

**This page shows use of redis query step available in this library**

## Parameters
* **name** [String] - Description of the Step.
* **query** [String] - Action to perform(flushall/set/delete). 
* **prefix** [String] - Provide prefix in case of deleting any key(Optional).
* **credentials** [REDIS] - Redis credentials

### Example 1
Below example waits for a file **temp/ratings.parquet** to be present in a S3 bucket named **s3_bucket**. 

     case class REDIS(url: String,user: String,password: String,port: Int)
 
     val step = RedisQueryStep(
         name           = "redis query step",
         query          = "flushall",
         credentials    = REDIS
       )
