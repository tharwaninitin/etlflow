---
layout: docs
title: GCS Steps
---

## Google Cloud Storage Steps

## GCSPutStep
**This step uploads file from local filesystem to GCS bucket in specified key.**
### Parameters
* **bucket** [String] - Name of the GCS bucket.
* **prefix** [String] - Prefix of the object. 
* **key** [String] - The key(folder) inside GCS bucket.
* **file** [String] - This is the full local path of the file which needs to be uploaded.

---
    val step = GCSPutStep(
        name    = "LoadRatingGCS",
        bucket  = "gcs_bucket",
        key     = "temp/ratings.parquet",
        file    = "local_file_path"
      )

## GCSDeleteStep
[Documentation coming soon]