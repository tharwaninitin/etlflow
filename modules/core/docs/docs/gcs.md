---
layout: docs
title: GCS Steps
---

## Google Cloud Storage Steps

## GCSPutStep

    val step = GCSPutStep(
        name    = "S3PutStep",
        bucket  = "gcs_bucket",
        key     = "temp/ratings.parquet",
        file    = "local_file_path"
      )
              
## GCSDeleteStep
[Documentation coming soon]