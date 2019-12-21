package etljob2

import EtlJobSchemas.{Rating, RatingOutput}
import com.google.cloud.bigquery.BigQueryOptions
import org.apache.spark.sql.functions.{col, from_unixtime, input_file_name}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoders, SparkSession, Dataset, SaveMode}
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep, SparkETLStep}
import etljobs.functions.SparkUDF
import etljobs.utils.{CSV, PARQUET, Settings}
import org.apache.log4j.{Level, Logger}
import etljobs.utils.SessionManager
import etljobs.spark.ReadApi

/**
* This example contains two steps and uses SessionManager for SparkSession and Bigquery 
* In first step it reads ratings data from ratings_input_path mentioned in input parameters
* then enrich it using function enrichRatingData and writes in PARQUET format at given output path
*
* In second step it reads PARQUET data stored by step1 and writes it to BigQuery table
*/

class EtlJobDefinition(val job_properties : Map[String,String]) extends EtlJob with SparkUDF {
  var output_date_paths : Seq[(String,String)] = Seq()
  val temp_date_col = "temp_date_col"
  Logger.getLogger("org").setLevel(Level.WARN)

  // Overriding Settings object to take local loaddata.properties
  override lazy val settings =  new Settings(new java.io.File(".").getCanonicalPath + "/etljobs/src/test/resources/loaddata.properties")

  /**
  * Enriches ratings dataset by adding columns date, date_int
  * and casting column date to date type

  * @param spark spark session
  * @param job_properties map of key value containing input parameters
  * @param in raw dataset which needs to be enriched
  * @return ratings enriched dataframe
  */
  def enrichRatingData(spark: SparkSession, job_properties : Map[String, String])(in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn(temp_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .where(f"$temp_date_col in ('20160101', '20160102')")

    ratings_df.as[RatingOutput](mapping)
  }

  def addFilePaths(spark: SparkSession, job_properties : Map[String, String])() = {
    import spark.implicits._

    output_date_paths = ReadApi.LoadDS[RatingOutput](Seq(job_properties("ratings_output_path")),PARQUET)(spark)
      .select(f"$temp_date_col")
      .withColumn("filename", input_file_name)
      .distinct()
      .as[(String,String)]
      .collect()
      .map((date) => (job_properties("ratings_output_path") + f"/$temp_date_col=" + date._1 + "/" + date._2.split("/").last, date._1))

    etl_job_logger.info("Filepaths generated are: ")
    output_date_paths.foreach(path => println(path))
  }

  val step1 = SparkReadWriteStep[Rating, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(",", true, job_properties.getOrElse("parse_mode","FAILFAST")),
    transform_function      = Some(enrichRatingData(spark, job_properties)),
    output_type             = PARQUET,
    output_location         = job_properties("ratings_output_path"),
    output_partition_col    = Some(f"$temp_date_col"),
    output_save_mode        = SaveMode.Overwrite,
    output_repartitioning   = true  // Setting this to true takes care of creating one file for every partition
  )(spark,job_properties)
  
  val step2 = new SparkETLStep(
    name                    = "GenerateFilePaths",
    transform_function      = addFilePaths(spark, job_properties)
  )(spark,job_properties)

  val step3 = new BQLoadStep(
    name                    = "LoadRatingBQ",
    source_paths_partitions = output_date_paths,
    destination_dataset     = job_properties("ratings_output_dataset"),
    destination_table       = job_properties("ratings_output_table_name"),
    source_format           = PARQUET
  )(bq,job_properties)

  def apply() : List[EtlStep[Unit,Unit]] = List(step1,step2,step3)
}
