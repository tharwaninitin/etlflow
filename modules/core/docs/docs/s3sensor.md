---
layout: docs
title: S3 Sensor
---

## AWS S3 Sensor Step

**This step runs S3 lookup for specified key in bucket repeatedly until it is found or fail after defined number of retries.**

## Parameters
* **bucket** [String] - Name of the GCS bucket.
* **prefix** [String] - Prefix of the object. 
* **key** [String] - The key being waited on. It is relative path from root level after prefix.
* **retry** [Int] - Number of retries before failing the step.
* **spaced** [Duration] - Each retry is done after waiting of duration specified in this field.

### Example 1
Below example waits for a file **temp/ratings.parquet** to be present in a S3 bucket named **s3_bucket**. 

     val step = S3SensorStep(
         name    = "S3KeySensorStep",
         bucket  = "s3_bucket",
         prefix  = "temp",
         key     = "ratings.parquet",
         retry   = 10,
         spaced  = 5.second,
         region  = Region.AP_SOUTH_1
       )
