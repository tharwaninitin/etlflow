---
layout: docs
title: Cloud Steps
---

## Cloud Steps

**This page shows different Cloud Steps available in this library**

### CloudStoreSyncStep : local to gcs
We can use below step when we want to store data from local storage to gcs bucket.

```scala mdoc
import etlflow.etlsteps.CloudStoreSyncStep
import etlflow.utils.Location

val gcs_output_location = "<GCS PATH>"
val gcs_bucket = "<GCS BUCKET>"
val step = CloudStoreSyncStep(
          name = "GCStoLOCALStep"
          , input_location = Location.LOCAL("modules/core/src/test/resources/input/movies/ratings/")
          , output_location = Location.GCS(gcs_bucket,gcs_output_location)
          , output_overwrite = true
          , parallelism = 3
          , chunk_size = 1000 * 1024
        )
```


### CloudStoreSyncStep : local to aws
We can use below step when we want to store data from local storage to aws bucket.

```scala mdoc
import etlflow.etlsteps.CloudStoreSyncStep
import etlflow.utils.Location
import software.amazon.awssdk.regions.Region

val s3_bucket = "<AWS BUCKET>"
val s3_input_location = "<AWS PATH>"
lazy val s3_region: Region = Region.AP_SOUTH_1

val step1 = CloudStoreSyncStep(
        name = "LOCALtoS3Step"
      , input_location = Location.LOCAL("modules/core/src/test/resources/input/movies/ratings/")
      , output_location = Location.S3(s3_bucket, s3_input_location, s3_region)
      , output_overwrite = true
      , chunk_size = 1000 * 1024
        )
```

### CloudStoreSyncStep : aws to local
We can use below step when we want to store data from aws bucket to local storage.

```scala mdoc
import etlflow.etlsteps.CloudStoreSyncStep
import etlflow.utils.Location

val step2 = CloudStoreSyncStep(
          name = "S3toLOCALStep"
          , input_location = Location.S3(s3_bucket, s3_input_location, s3_region)
          , output_location = Location.LOCAL("modules/core/src/test/resources/s3_output/")
          , output_overwrite = true
        )
```


### CloudStoreSyncStep : gcs to local
We can use below step when we want to store data from gcs bucket to local storage.

```scala mdoc
import etlflow.etlsteps.CloudStoreSyncStep
import etlflow.utils.Location

val gcs_input_location = "<GCS PATH>"

val step3 = CloudStoreSyncStep(
          name = "GCStoLOCALStep"
          , input_location = Location.GCS(gcs_bucket, gcs_input_location)
          , output_location = Location.LOCAL("modules/core/src/test/resources/gcs_output/")
          , output_overwrite = true
          , parallelism = 3
)
```
