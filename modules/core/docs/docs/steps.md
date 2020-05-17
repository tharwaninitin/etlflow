---
layout: docs
title: EtlSteps
---

## EtlSteps

**This page shows some of the most commonly used steps from this library**

**Initial Setup**

     import etlflow.etlsteps._
     import etlflow.utils._
     
     import org.apache.spark.sql.SparkSession
     import org.apache.spark.sql.SaveMode
     import org.apache.spark.sql.{Encoders, Dataset}
     import org.apache.spark.sql.types.DateType
     import org.apache.spark.sql.functions._
     import com.google.cloud.bigquery.JobInfo
     
     val canonical_path: String = new java.io.File(".").getCanonicalPath
     
     val job_properties: Map[String,String] = Map(
         "ratings_input_path" -> s"$canonical_path/modules/examples/src/main/data/movies/ratings/*",
         "ratings_output_path" -> s"$canonical_path/modules/examples/src/main/data/movies/output/ratings",
         "ratings_output_file_name" -> "ratings.orc",
         "ratings_output_dataset" -> "test",
         "ratings_output_table_name" -> "ratings"
     )
     
     case class Rating(user_id:Int, movie_id: Int, rating : Double, timestamp: Long)
     case class RatingOutput(user_id:Int, movie_id: Int, rating: Double, timestamp: Long, date: java.sql.Date)

## SparkReadWriteStep
    
     val step = SparkReadWriteStep[Rating](
        name             = "LoadRatingsParquetToJdbc",
        input_location   = Seq(job_properties("ratings_input_path")),
        input_columns    = Seq("user_id","movie_id","rating","timestamp"),
        input_type       = PARQUET,
        output_type      = JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "jdbc_driver"),
        output_location  = job_properties("ratings_output_table_name"),
        output_save_mode = SaveMode.Overwrite
     )
      
## SparkReadTransformWriteStep

     def enrichRatingData(spark: SparkSession, in : Dataset[Rating]): Dataset[RatingOutput] = {
          val mapping = Encoders.product[RatingOutput]
      
          val ratings_df = in
              .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
          
          ratings_df.as[RatingOutput](mapping)
      }
     
     val step1 = SparkReadTransformWriteStep[Rating, RatingOutput](
         name                  = "LoadRatingsParquet",
         input_location        = Seq(job_properties("ratings_input_path")),
         input_type            = CSV(",", true, "FAILFAST"),
         transform_function    = enrichRatingData,
         output_type           = PARQUET,
         output_location       = job_properties("ratings_output_path"),
         output_save_mode      = SaveMode.Overwrite,
         output_partition_col  = Seq("date"),
         output_repartitioning = true  // Setting this to true takes care of creating one file for every partition
     )
     
## BQLoadStep
    
     val select_query: String = """
          | SELECT movie_id, COUNT(1) cnt
          | FROM test.ratings
          | GROUP BY movie_id
          | ORDER BY cnt DESC;
          |""".stripMargin

     val step1 = BQLoadStep(
         name            = "LoadQueryDataBQ",
         input_location  = Left(select_query),
         input_type      = BQ,
         output_dataset  = "test",
         output_table    = "ratings_grouped",
         output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
     )
     
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
     
     val step2 = BQLoadStep(
         name           = "LoadQueryDataBQPar",
         input_location = Right(input_query_partitions),
         input_type     = BQ,
         output_dataset = "test",
         output_table   = "ratings_grouped_par"
     ) 

## BQQueryStep
   
    val step = BQQueryStep(
        name  = "CreateTableBQ",
        query = s"""CREATE OR REPLACE TABLE test.ratings_grouped as
                SELECT movie_id, COUNT(1) cnt
                FROM test.ratings
                GROUP BY movie_id
                ORDER BY cnt DESC;""".stripMargin
    )     
      
## DBQueryStep

    val step4 = DBQueryStep(
        name  = "UpdatePG",
        query = "BEGIN; DELETE FROM ratings WHERE 1 =1; INSERT INTO ratings SELECT * FROM ratings_temp; COMMIT;",
        credentials = JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "org.postgresql.Driver")
    )
