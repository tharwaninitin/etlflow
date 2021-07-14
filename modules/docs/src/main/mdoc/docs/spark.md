---
layout: docs
title: Spark
---

## Apache Spark Steps

**This page shows different Spark Steps available in this library**

Below are the Input/Output formats supported by this step: 
* Input Formats => **CSV, JSON, ORC, PARQUET, JDBC, BQ**
* Output Formats  => **CSV, JSON, ORC, PARQUET, JDBC**

### SparkReadWriteStep
We can use below step when : 
* We want load the data from above mentioned source format into destination format.
* When there is no need of transformation function.

**Create spark session**   

```scala mdoc
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}
LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
implicit lazy val spark: SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()       
       
```

```scala mdoc

import etlflow.etlsteps._
import etlflow.utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{Encoders, Dataset}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import etlflow.spark.IOType
import etlflow.gcp.BQInputType
import etlflow.schema.Credential.JDBC

case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long)

lazy val step1 = SparkReadWriteStep[Rating](
        name             = "LoadRatingsParquetToJdbc",
        input_location   = Seq("gs://path/to/input/*"),
        input_type       = IOType.PARQUET,
        output_type      = IOType.RDB(JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "jdbc_driver")),
        output_location  = "ratings",
        output_save_mode = SaveMode.Overwrite
)
```
      
### SparkReadTransformWriteStep
We can use below step when :
* We want load the data from above mentioned source format into destination format.
* When there is need of transformation function.

```scala mdoc
   
case class RatingOutputCsv(user_id: Int, movie_id: Int, rating : Double, timestamp: Long, date: java.sql.Date)


def enrichRatingCsvData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutputCsv] = {
            val mapping = Encoders.product[RatingOutputCsv]
        
            val ratings_df = in
                .withColumnRenamed("user_id","User Id")
                .withColumnRenamed("movie_id","Movie Id")
                .withColumnRenamed("rating","Ratings")
                .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
                .withColumnRenamed("date","Movie Date")
        
            ratings_df.as[RatingOutputCsv](mapping)
}

lazy val step3 = SparkReadTransformWriteStep[Rating, RatingOutputCsv](
          name                  = "LoadRatingsCsvToCsv",
          input_location        = Seq("gs://path/to/input/"),
          input_type            = IOType.CSV(),
          transform_function    = enrichRatingCsvData,
          output_type           = IOType.CSV(),
          output_location       = "gs://path/to/output/",
          output_save_mode      = SaveMode.Overwrite,
          output_filename       = Some("ratings.csv")
)
```
**Whenever we want to write the data using above mentioned steps into partitioned format then dont specify the output_filename as a parameter**     