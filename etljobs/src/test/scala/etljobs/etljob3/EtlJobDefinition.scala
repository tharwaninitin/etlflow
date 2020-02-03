package etljobs.etljob3

import EtlJobSchemas.{EtlJob3Props, Rating, RatingOutput}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import etljobs.{EtlJob, EtlJobName, EtlProps}
import etljobs.etlsteps.{BQLoadStep, DatasetWithState, EtlStep, SparkReadWriteStateStep}
import etljobs.functions.SparkUDF
import etljobs.utils.{CSV, GlobalProperties, PARQUET}
import org.apache.log4j.{Level, Logger}

class EtlJobDefinition(
                        val job_name: EtlJobName = etljobs.EtlJobList.EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,
                        val job_properties: Either[Map[String,String], EtlProps],
                        val global_properties: Option[GlobalProperties] = None
                      )
  extends EtlJob with SparkUDF {
  var output_date_paths : Seq[(String,String)] = Seq()
  val temp_date_col = "temp_date_col"
  Logger.getLogger("org").setLevel(Level.WARN)

  val job_props:EtlJob3Props  = job_properties match {
    case Right(value) => value.asInstanceOf[EtlJob3Props]
  }

  def enrichRatingData(spark: SparkSession, job_properties: EtlJob3Props)(in : DatasetWithState[Rating,Unit]) : DatasetWithState[RatingOutput,Unit] = {

    val ratings_df = in.ds
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn(temp_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .where(f"$temp_date_col in ('20160101', '20160102')")

    import spark.implicits._

    output_date_paths = ratings_df
        .select(f"$temp_date_col")
        .distinct()
        .as[String]
        .collect()
        .map(date => (job_properties.ratings_output_path + f"/$temp_date_col=" + date + "/part*", date))

    ratings_df.drop(f"$temp_date_col")

    val mapping = Encoders.product[RatingOutput]
    val ratings_ds = ratings_df.as[RatingOutput](mapping)

    DatasetWithState(ratings_ds,())
  }

  val step1 = SparkReadWriteStateStep[Rating, Unit, RatingOutput, Unit](
    name                   = "LoadRatingsParquet",
    input_location         = Seq(job_props.ratings_input_path),
    input_type             = CSV(",", true, "FAILFAST"),
    transform_with_state   = Some(enrichRatingData(spark, job_props)),
    output_type            = PARQUET,
    output_location        = job_props.ratings_output_path,
    output_partition_col   = Some(f"$temp_date_col"),
    output_save_mode       = SaveMode.Overwrite,
    output_repartitioning  = true  // Setting this to true takes care of creating one file for every partition
  )(spark)

  val step2 = BQLoadStep(
    name                    = "LoadRatingBQ",
    source_paths_partitions = output_date_paths,
    source_format           = PARQUET,
    destination_dataset     = job_props.ratings_output_dataset,
    destination_table       = job_props.ratings_output_table_name
  )(bq)

  val etl_step_list: List[EtlStep[Unit,Unit]] = List(step1,step2)
}
