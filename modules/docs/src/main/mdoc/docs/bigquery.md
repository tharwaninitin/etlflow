---
layout: docs
title: BigQuery
---

## Google Cloud BigQuery Steps

**This page shows different BigQuery Steps available in this library**

## BQLoadStep
Below are the Input/Output formats supported by this step: 
* Input Formats => **CSV, JSON, ORC, PARQUET, BQ**
* Output Formats => **BQ**

### Example 1 (Source=>CSV on GCS, Destination=>BQ)
Here input is CSV file on GCS location with schema specified by case class Rating and output is BigQuery table.

```scala mdoc
import etlflow.etlsteps.BQLoadStep
import etlflow.spark.IOType
import etlflow.gcp.BQInputType
 
sealed trait MyEtlJobSchema
case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long) extends MyEtlJobSchema

val step1 = BQLoadStep[Rating](
name           = "LoadRatingBQ",
input_location = Left("gs://path/to/input/*"),
input_type     = BQInputType.CSV(),
output_dataset = "test", 
output_table   = "ratings"
)
````
     
### Example 2 (Source=>BQ, Destination=>BQ)
Here input is BigQuery SQL and output is BigQuery table.

```scala mdoc
import etlflow.etlsteps.BQLoadStep
import com.google.cloud.bigquery.{FormatOptions, JobInfo}
import etlflow.spark.IOType
import etlflow.gcp.BQInputType

val select_query: String = """
          | SELECT movie_id, COUNT(1) cnt
          | FROM test.ratings
          | GROUP BY movie_id
          | ORDER BY cnt DESC;
          |""".stripMargin

val step2 = BQLoadStep(
         name            = "LoadQueryDataBQ",
         input_location  = Left(select_query),
         input_type      = BQInputType.BQ,
         output_dataset  = "test",
         output_table    = "ratings_grouped",
         output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
)
```
     
### Example 3 (Source=>Seq(BQ), Destination=>BQ)
Here input is sequence of BigQuery SQL with date partition and output is again BigQuery table. 
Using this step we can load multiple different partitions in parallel in BigQuery.
     
```scala mdoc
import etlflow.etlsteps.BQLoadStep
import com.google.cloud.bigquery.{FormatOptions, JobInfo}
import etlflow.spark.IOType
import etlflow.gcp.BQInputType
     
val getQuery: String => String = param => s"""
          | SELECT date, movie_id, COUNT(1) cnt
          | FROM test.ratings_par
          | WHERE date = '$param'
          | GROUP BY date, movie_id
          | ORDER BY cnt DESC;
          |""".stripMargin
          
val input_query_partitions = Seq(
          (getQuery("2016-01-01"),"20160101"),
          (getQuery("2016-01-02"),"20160102")
)
     
val step3 = BQLoadStep(
         name           = "LoadQueryDataBQPar",
         input_location = Right(input_query_partitions),
         input_type     = BQInputType.BQ,
         output_dataset = "test",
         output_table   = "ratings_grouped_par"
) 
```

## BQQueryStep
We can use below step when we want to just run some query/stored-procedure on BigQuery without returning anything.

```scala mdoc
import etlflow.etlsteps.BQQueryStep
import com.google.cloud.bigquery.{FormatOptions, JobInfo}
import etlflow.spark.IOType
import etlflow.gcp.BQInputType

val step4 = BQQueryStep(
        name  = "CreateTableBQ",
        query = s"""CREATE OR REPLACE TABLE test.ratings_grouped as
                SELECT movie_id, COUNT(1) cnt
                FROM test.ratings
                GROUP BY movie_id
                ORDER BY cnt DESC;""".stripMargin
)     
```      