---
layout: docs
title: Getting started
---

## Getting Started

Clone this git repo and go inside repo root folder and enter below command (make sure you have sbt and scala installed)

    SBT_OPTS="-Xms512M -Xmx1024M -Xss2M -XX:MaxMetaspaceSize=1024M" sbt -v "project etljobs" console

**Import core packages**

    import etljobs.etlsteps.SparkReadWriteStep
    import etljobs.utils.{CSV,ORC}
    
**Define Job input and ouput locations**

    val canonical_path: String = new java.io.File(".").getCanonicalPath
    val job_properties: Map[String,String] = Map(
        "ratings_input_path" -> f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
        "ratings_output_path" -> f"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
        "ratings_output_file_name" -> "ratings.orc"
      )

**Create spark session**   
   
    import org.apache.spark.sql.SparkSession
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)
    
    lazy val spark: SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()
      

**Define ETL Step which will load ratings data with below schema as specified from CSV to ORC**          
          
    import org.apache.spark.sql.SaveMode
    case class Rating(user_id:Int, movie_id: Int, rating : Double, timestamp: Long)
    
    val step1 = SparkReadWriteStep[Rating, Rating](
        name                    = "ConvertRatingsCSVtoORC",
        input_location          = Seq(job_properties("ratings_input_path")),
        input_type              = CSV(),
        output_location         = job_properties("ratings_output_path"),
        output_type             = ORC,
        output_save_mode        = SaveMode.Overwrite,
        output_filename         = Some(job_properties("ratings_output_file_name"))
     )(spark)
   
**Run this step individually as below**

     step1.process()
       
Now executing this step will add data in parquet format in path defined in above properties upon completion. This is very basic example for flat load from CSV to ORC but lets say you need to transform csv data in some way for e.g. new column need to be added then we need to create function with below signature:   
        
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
       
**Now our step would change to something like this**
       
     val step1 = SparkReadWriteStep[Rating, RatingOutput](
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
       
**Lets add another step which will copy this transformed data in Bigquery table. For this step to work correctly [Google Cloud SDK](https://cloud.google.com/sdk/install) needs to be installed and configured as in this library upload from local file to Bigquery uses [bq command](https://cloud.google.com/bigquery/docs/bq-command-line-tool) which is only recommended to be used in testing environments as in production files should be present on **Google Cloud Storage** when uploading to Bigquery**

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
        input_location      = Left(job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name")),
        input_type          = ORC,
        input_file_system   = LOCAL,
        output_dataset      = job_properties("ratings_output_dataset"),
        output_table        = job_properties("ratings_output_table_name")
      )(bq)
    
    step2.process()

**Now we can run this individually like previous step or use ETLJob api to run both of these steps as single job. You can find similar code [here](modules/examples/src/main/scala/examples) along with **tests****



