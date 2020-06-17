---
layout: docs
title: GCS Sensor
---

## Google Cloud Storage Sensor Step

**This step runs a GCS lookup for specified key in bucket repeatedly until it is found or fail after defined number of retries.**

## Parameters
* **bucket** [String] - Name of the GCS bucket.
* **prefix** [String] - Prefix of the object. 
* **key** [String] - The key being waited on. It is relative path from root level after prefix.
* **retry** [Int] - Number of retries before failing the step.
* **spaced** [Duration] - Each retry is done after waiting of duration specified in this field.

### Example 1
Below example waits for a file **temp/ratings.parquet** to be present in a GCS bucket named **gcs_bucket**. 

     val step = GCSSensorStep(
         name    = "GCSKeySensor",
         bucket  = "gcs_bucket",
         prefix  = "temp",
         key     = "ratings.parquet",
         retry   = 10,
         spaced  = 5.second
      )
     