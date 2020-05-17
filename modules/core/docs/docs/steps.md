---
layout: docs
title: Etl Steps
---

## Etl Steps

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

     val step = SparkReadTransformWriteStep[HotstarMasterMappingAws, HotstarMasterMappingBQ](
        name                    = "load_hostar_master_mapping_GCP",
        input_location          = Seq(props.job_input_path),
        input_type              = CSV(",",true),
        output_location         = props.job_output_path,
        transform_function      = hotstar_master_mapping_transform,
        output_type             = ORC,
        output_save_mode        = SaveMode.Overwrite,
        output_filename         = Some(props.output_file_name)
      )
      
**SparkETLStep**       
          
    val step = new SparkETLStep(
        name="POSTGRES_UPDATE_FACT_REV_TABLE",
        transform_function=queryPostgres
    )
     
**BQLoadStep**

     val step = BQLoadStep(
        name                = "LoadRatingBQ",
        source_path         = job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name"),
        source_format       = PARQUET,
        source_file_system  = LOCAL,
        destination_dataset = job_properties("ratings_output_dataset"),
        destination_table   = job_properties("ratings_output_table_name")
      )
    

**BQQueryStep**   
   
    private val query = s"""create table ${props.output_dataset}.${props.output_table_name} as select advertiser_group, channel_name from ${props.output_dataset}.${props.job_input_path} limit 10;"""
    
    private val step1 = BQQueryStep(
      name = "CreateStoredProcedure",
      query = query
    )      
