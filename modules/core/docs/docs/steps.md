---
layout: docs
title: EtlSteps
---

## EtlSteps

**SparkReadWriteStep**
    
     val step = SparkReadWriteStep[Rating](
        name             = "LoadRatingsParquetToJdbc",
        input_location   = job_props.ratings_input_path,
        input_columns    = Seq("user_id","movie_id","rating","timestamp"),
        input_type       = PARQUET,
        output_type      = JDBC(global_props.jdbc_url, global_props.jdbc_user, global_props.jdbc_pwd, global_props.jdbc_driver),
        output_location  = job_props.ratings_output_table,
        output_save_mode = SaveMode.Overwrite
      )
      
**SparkReadTransformWriteStep**

     val step1 = SparkReadTransformWriteStep[Rating, RatingOutput](
         name                  = "LoadRatingsParquet",
         input_location        = Seq(job_props.ratings_input_path),
         input_type            = CSV(",", true, "FAILFAST"),
         transform_function    = enrichRatingData,
         output_type           = PARQUET,
         output_location       = gcs_output_path,
         output_save_mode      = SaveMode.Overwrite,
         output_partition_col  = Seq(f"$temp_date_col"),
         output_repartitioning = true  // Setting this to true takes care of creating one file for every partition
       )
      
**SparkETLStep**       
          
    val step = new SparkETLStep(
        name               = "GenerateFilePaths",
        transform_function = addFilePaths(job_props)
      ) {
        override def getStepProperties(level: String) : Map[String,String] = Map("paths" -> output_date_paths.mkString(","))
      }
     
**BQLoadStep**

     val step1 = BQLoadStep(
         name            = "LoadQueryDataBQ",
         input_location  = Left(select_query),
         input_type      = BQ,
         output_dataset  = "test",
         output_table    = "ratings_grouped",
         output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
     )
     
     val step2 = BQLoadStep(
         name           = "LoadQueryDataBQPar",
         input_location = Right(input_query_partitions),
         input_type     = BQ,
         output_dataset = "test",
         output_table   = "ratings_grouped_par"
     )
    

**BQQueryStep**   
   
    val step = BQQueryStep(
        name  = "CreateTableBQ",
        query = s"""CREATE OR REPLACE TABLE test.ratings_grouped as
                SELECT movie_id, COUNT(1) cnt
                FROM test.ratings
                GROUP BY movie_id
                ORDER BY cnt DESC;""".stripMargin
    )     
      
**DBQueryStep**

    val step4 = DBQueryStep(
        name  = "UpdatePG",
        query = "BEGIN; DELETE FROM ratings WHERE 1 =1; INSERT INTO ratings SELECT * FROM ratings_temp; COMMIT;",
        credentials = JDBC(global_props.log_db_url, global_props.log_db_user, global_props.log_db_pwd, global_props.jdbc_driver)
    )
