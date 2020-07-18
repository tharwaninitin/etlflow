---
layout: docs
title: Quick Start
---

## Quick Start

Clone this git repo and go inside repo root folder and enter below command (make sure you have sbt and scala installed)

    SBT_OPTS="-Xms512M -Xmx1024M -Xss2M -XX:MaxMetaspaceSize=1024M" sbt -v "project examples" console

**Import core packages**

    import etlflow.etlsteps._
    import etlflow.utils._
    
**Define Job input and ouput locations**

    val canonical_path: String = new java.io.File(".").getCanonicalPath
    val job_properties: Map[String,String] = Map(
        "ratings_input_path" -> s"$canonical_path/examples/src/main/data/movies/ratings/*",
        "ratings_output_path" -> s"$canonical_path/examples/src/main/data/movies/output/ratings",
        "ratings_output_file_name" -> "ratings.orc"
    )

**Create spark session**   
   
    import org.apache.spark.sql.SparkSession
    import org.slf4j.LoggerFactory
    import ch.qos.logback.classic.{Level, Logger}
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    implicit lazy val spark: SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()

**Define ETL Step which will load ratings data with below schema as specified from CSV to ORC**          
          
    import org.apache.spark.sql.SaveMode
    
    case class Rating(user_id:Int, movie_id: Int, rating : Double, timestamp: Long)
    
    val step1 = SparkReadWriteStep[Rating](
        name                       = "ConvertRatingsCSVtoORC",
        input_location             = Seq(job_properties("ratings_input_path")),
        input_type                 = CSV(),
        output_location            = job_properties("ratings_output_path"),
        output_type                = ORC,
        output_save_mode           = SaveMode.Overwrite,
        output_repartitioning_num  = 1,
        output_repartitioning      = true,
        output_filename            = Some(job_properties("ratings_output_file_name"))
    )
     
**Since all these steps return Task from ZIO library, we need to import Zio Default Runtime to run these steps**
     
     import zio.{Runtime,ZEnv}
     val runtime: Runtime[ZEnv] = Runtime.default
          
**Run this step as below**

     val task = step1.process()
     runtime.unsafeRun(task)
       
**Now executing above step has added data in ORC format in path defined in above properties upon completion.** 

**This is very basic example for flat load from CSV to ORC but lets say you need to transform csv data in some way for e.g. new column need to be added then we need to create function with below signature:**
        
     import org.apache.spark.sql.{Encoders, Dataset}
     import org.apache.spark.sql.types.DateType
     import org.apache.spark.sql.functions._
     
     case class RatingOutput(user_id:Int, movie_id: Int, rating: Double, timestamp: Long, date: java.sql.Date)
     
     def enrichRatingData(spark: SparkSession, in : Dataset[Rating]): Dataset[RatingOutput] = {
         val mapping = Encoders.product[RatingOutput]
     
         val ratings_df = in
             .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
         
         ratings_df.as[RatingOutput](mapping)
     }
       
**Now our step would change to something like this**
     
     val step1 = SparkReadTransformWriteStep[Rating, RatingOutput](
         name                       = "ConvertRatingsCSVtoORC",
         input_location             = Seq(job_properties("ratings_input_path")),
         input_type                 = CSV(),
         transform_function         = enrichRatingData,
         output_type                = ORC,
         output_save_mode           = SaveMode.Overwrite,
         output_location            = job_properties("ratings_output_path"),
         output_repartitioning_num  = 1,
         output_repartitioning      = true,
         output_filename            = Some(job_properties("ratings_output_file_name"))
     )
     
     val task = step1.process()
     runtime.unsafeRun(task)
       
**Lets add another step which will copy this transformed ORC data in BigQuery table. 
For this step to work correctly [Google Cloud SDK](https://cloud.google.com/sdk/install) needs to be installed and configured, 
as in this library upload from local file to BigQuery uses [bq command](https://cloud.google.com/bigquery/docs/bq-command-line-tool) which is only recommended to be used in testing environments as in production files should be present on Google Cloud Storage when uploading to BigQuery**
    
    // Adding two new properties for Bigquery table and Dataset
    val job_properties: Map[String,String] = Map(
        "ratings_input_path" -> s"$canonical_path/examples/src/main/data/movies/ratings/*",
        "ratings_output_path" -> s"$canonical_path/examples/src/main/data/movies/output/ratings",
        "ratings_output_file_name" -> "ratings.orc",
        "ratings_output_dataset" -> "test",
        "ratings_output_table_name" -> "ratings"
    )
    
    val step2 = BQLoadStep(
        name                = "LoadRatingBQ",
        input_location      = Left(job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name")),
        input_type          = ORC,
        input_file_system   = LOCAL,
        output_dataset      = job_properties("ratings_output_dataset"),
        output_table        = job_properties("ratings_output_table_name")
    )
    
    val task = step2.process()
    runtime.unsafeRun(task)

**Now we can run also chain multiple steps together and run as single job as shown below.**

    val job = for {
        _ <- step1.process()
        _ <- step2.process()
    } yield ()
    
    runtime.unsafeRun(job)



