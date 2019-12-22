Etljobs
====

**Etljobs** is a library that help with faster ETL development using **Apache Spark**. At a very high level,
the library provides abstraction on top of Spark that makes it easier to develop ETL applications which can be easily tested and composed. This library has **Google Bigquery** support as both ETL source and destination.

The project contains the following sub-modules:

1. **etljobs**:
 This module contains core library which defines Scala internal **dsl** that assists with writing **ETL Job** which can be composed as multiple **ETL steps** in a concise manner which facilitates easier **Testing** and reasoning about the entire job. This module also conatins many [test jobs](etljobs/src/test/scala) which conatins multiple steps
2. **examples**:
 This module provides examples of diffferent types of ETL Jobs which can be created with this library, click [here](examples/src/main/scala/examples) to see code.

## Getting Started
1. Import core packages:
```scala
import etljobs.etlsteps.SparkReadWriteStep
import etljobs.utils.{CSV,ORC}
```
2. Define Job input and ouput locations:
```scala
val job_properties: Map[String,String] = Map(
    "ratings_input_path" -> "gs://<some_bucket>/input/ratings/*",
    "ratings_output_path" -> "gs://<some_bucket>/output/ratings",
  )
```
3. Create spark session:
```scala
val spark: SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()
```
4. Define ETL Step which will load ratings data with below schema as specified from CSV to PARQUET:
```scala
case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )

val step1 = new SparkReadWriteStep[Rating, Rating](
    name                    = "ConvertRatingsCSVtoParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(),
    output_location         = job_properties("ratings_output_path"),
    output_type             = ORC
 )(spark,job_properties)
```
5. Run this step individually as below:
```scala
step1.process()
```
Now executing this step will add data in parquet format in path defined in above properties upon completion. This is very basic example for flat load from CSV to PARQUET but lets say you need to transform csv data in some way for e.g. new column need to be added then we need to create function with below signature:
```scala
def enrichRatingData(spark: SparkSession, job_properties : Map[String, String])(in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
    
    ratings_df.as[RatingOutput](mapping)
  }
```
Now our step would change to something like this:
```scala
case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )
case class RatingOutput(user_id:Int, movie_id: Int, rating: Double, timestamp: Long, date: java.sql.Date)

 val step1 = new SparkReadWriteStep[Rating, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(),
    transform_function      = Some(enrichRatingData(spark, job_properties)),
    output_type             = ORC,
    output_location         = job_properties("ratings_output_path"),
  )(spark,job_properties)
```
6. Lets add another step which will copy this transformed data in Bigquery table
```scala
import etljobs.etlsteps.BQLoadStep
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
  
val bq: BigQuery = BigQueryOptions.getDefaultInstance.getService

// Adding two new properties for Bigquery table and Dataset
val job_properties: Map[String,String] = Map(
    "ratings_input_path" -> "gs://<some_bucket>/input/ratings/*",
    "ratings_output_path" -> "gs://<some_bucket>/output/ratings",
    "ratings_output_dataset" -> "some_dataset",
    "ratings_output_table_name" -> "ratings"
  )

val step2 = BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_properties("ratings_output_path"),
    source_format       = ORC,
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name")
  )(bq,job_properties)
```
Now we can run this individually like previous step or use ETLJob api to run both of these steps as single job. Find full code [here](etljobs/src/test/scala/etljob1) along with **tests**


## Requirements and Installation
This project is compiled with scala 2.11.12 and works with Apache Spark versions 2.4.x.
Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etljobs). 
Add the latest release as a dependency to your project

__Maven__
```
<dependency>
    <groupId>com.github.tharwaninitin</groupId>
    <artifactId>etljobs_2.11</artifactId>
    <version>0.4.0</version>
</dependency>
```
__SBT__
```
libraryDependencies += "com.github.tharwaninitin" %% "etljobs" % "0.4.0"
```
__Download Latest JAR__ https://github.com/tharwaninitin/etljobs/releases/tag/v0.4.0


## Documentation

__Scala API Docs__ https://tharwaninitin.github.io/etljobs/api/

__Scala Test Coverage Report__  https://tharwaninitin.github.io/etljobs/testcovrep/

#### Contributions
Please feel free to add issues to report any bugs or to propose new features.
