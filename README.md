Etljobs
====

**Etljobs** is a library that help with faster ETL development using **Apache Spark**. At a very high level,
the library provides abstraction on top of Spark that makes it easier to develop ETL applications which can be easily **Tested** and **Composed**. This library has **Google Bigquery** support as both ETL source and destination.

This project contains following sub-modules:

1. **etljobs**:
 This module contains core library which defines Scala internal **dsl** that assists with writing **ETL Job** which can be composed as multiple **ETL steps** in a concise manner which facilitates easier **Testing** and reasoning about the entire job. This module also conatins many [test jobs](core/src/test/scala) which conatins multiple steps. This core library also contains tests and all jobs uses EtlJob API. To run all test successfully some properties needs to be set in [loaddata.properties](core/src/test/resources/loaddata.properties) or set these properties as ENVIRONMENT variables.
 ```shell
 export GCS_OUTPUT_BUCKET=<...>
 export GCP_PROJECT=<...>
 export GCP_PROJECT_KEY_NAME=<...> # this should be full path to Service Account Key Json which should have GCS and Biguery Read/Write access
 ```
 Now run tests using below sbt command
 ```shell
sbt "project etljobs" test
```
2. **etlsteps**:
 This package contains all type of ETL Steps that can be created with this library, click [here](core/src/main/scala/etljobs/etlsteps) to see.
3. **examples**:
 This module provides examples of diffferent types of ETL Jobs which can be created with this library, click [here](examples/src/main/scala/examples) to see code.

## Getting Started
Clone this git repo and go inside repo root folder and enter below command (make sure you have sbt and scala installed)
```shell
SBT_OPTS="-Xms512M -Xmx1024M -Xss2M -XX:MaxMetaspaceSize=1024M" sbt -v "project etljobs" console
```
1. Import core packages:
```scala
import etljobs.etlsteps.SparkReadWriteStep
import etljobs.utils.{CSV,ORC}
```
2. Define Job input and ouput locations:
```scala
val canonical_path: String = new java.io.File(".").getCanonicalPath
val job_properties: Map[String,String] = Map(
    "ratings_input_path" -> f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
    "ratings_output_path" -> f"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
    "ratings_output_file_name" -> "ratings.orc"
  )
```
3. Create spark session:
```scala
import org.apache.spark.sql.SparkSession
org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)

lazy val spark: SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()
```
4. Define ETL Step which will load ratings data with below schema as specified from CSV to ORC:
```scala
import org.apache.spark.sql.SaveMode
case class Rating(user_id:Int, movie_id: Int, rating : Double, timestamp: Long)

val step1 = new SparkReadWriteStep[Rating, Rating](
    name                    = "ConvertRatingsCSVtoORC",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(),
    output_location         = job_properties("ratings_output_path"),
    output_type             = ORC,
    output_save_mode        = SaveMode.Overwrite,
    output_filename         = Some(job_properties("ratings_output_file_name"))
 )(spark)
```
5. Run this step individually as below:
```scala
step1.process()
```
Now executing this step will add data in parquet format in path defined in above properties upon completion. This is very basic example for flat load from CSV to ORC but lets say you need to transform csv data in some way for e.g. new column need to be added then we need to create function with below signature:
```scala
import org.apache.spark.sql.{Encoders, Dataset}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._

case class RatingOutput(user_id:Int, movie_id: Int, rating: Double, timestamp: Long, date: java.sql.Date)

def enrichRatingData()(in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
    
    ratings_df.as[RatingOutput](mapping)
  }
```
Now our step would change to something like this:
```scala
 val step1 = new SparkReadWriteStep[Rating, RatingOutput](
    name                    = "ConvertRatingsCSVtoORC",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(),
    transform_function      = Some(enrichRatingData()),
    output_type             = ORC,
    output_save_mode        = SaveMode.Overwrite,
    output_location         = job_properties("ratings_output_path"),
    output_filename         = Some(job_properties("ratings_output_file_name"))
  )(spark)

  step1.process()
```
6. Lets add another step which will copy this transformed data in Bigquery table. For this step to work correctly [Google Cloud SDK](https://cloud.google.com/sdk/install) needs to be installed and configured as in this library upload from local file to Bigquery uses [bq command](https://cloud.google.com/bigquery/docs/bq-command-line-tool) which is only recommended to be used in testing environments as in production files should be present on **Google Cloud Storage** when uploading to Bigquery, see this [page](core/src/main/scala/etljobs/etlsteps) for more details on each Step
```scala
import etljobs.etlsteps.BQLoadStep
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import etljobs.utils.LOCAL

val bq: BigQuery = BigQueryOptions.getDefaultInstance.getService

// Adding two new properties for Bigquery table and Dataset
val job_properties: Map[String,String] = Map(
    "ratings_input_path" -> f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
    "ratings_output_path" -> f"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
    "ratings_output_file_name" -> "ratings.orc",
    "ratings_output_dataset" -> "test",
    "ratings_output_table_name" -> "ratings"
  )

val step2 = BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name"),
    source_format       = ORC,
    source_file_system  = LOCAL,
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name")
  )(bq)

step2.process()
```
Now we can run this individually like previous step or use ETLJob api to run both of these steps as single job. You can find similar code [here](core/src/test/scala/etljobs.etljob1) along with **tests**


## Requirements and Installation
This project is compiled with scala 2.11.12 and works with Apache Spark versions 2.4.x.
Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etljobs). 
Add the latest release as a dependency to your project

__Maven__
```
<dependency>
    <groupId>com.github.tharwaninitin</groupId>
    <artifactId>etljobs_2.11</artifactId>
    <version>0.7.0</version>
</dependency>
```
__SBT__
```
libraryDependencies += "com.github.tharwaninitin" %% "etljobs" % "0.7.0"
```
__Download Latest JAR__ https://github.com/tharwaninitin/etljobs/releases/tag/v0.7.0


## Documentation

__Scala API Docs__ https://tharwaninitin.github.io/etljobs/api/

__Scala Test Coverage Report__  https://tharwaninitin.github.io/etljobs/testcovrep/

#### Contributions
Please feel free to add issues to report any bugs or to propose new features.
