package etljobs.etljob3

import etljobs.bigquery.BigQueryManager
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import etljobs.{EtlJob, EtlJobName, EtlJobProps, EtlStepList}
import etljobs.etlsteps.{BQLoadStep, DatasetWithState, SparkReadTransformWriteStateStep, SparkReadWriteStateStep, StateLessEtlStep}
import etljobs.schema.MyEtlJobProps.EtlJob23Props
import etljobs.schema.EtlJobSchemas.{Rating, RatingOutput}
import etljobs.spark.{SparkManager, SparkUDF}
import etljobs.utils.{CSV, GlobalProperties, PARQUET}
import org.apache.log4j.{Level, Logger}

class EtlJobDefinition(
                        val job_name: String,
                        val job_properties: EtlJobProps,
                        val global_properties: Option[GlobalProperties] = None
                      )
  extends EtlJob with SparkManager with SparkUDF with BigQueryManager {
  var output_date_paths: Seq[(String,String)] = Seq()
  val temp_date_col = "temp_date_col"
  Logger.getLogger("org").setLevel(Level.WARN)

  val job_props:EtlJob23Props  = job_properties.asInstanceOf[EtlJob23Props]

  def enrichRatingData(spark: SparkSession, job_properties: EtlJob23Props)(in : DatasetWithState[Rating,Unit]) : DatasetWithState[RatingOutput,Unit] = {

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

  val step1 = SparkReadTransformWriteStateStep[Rating, Unit, RatingOutput, Unit](
    name                  = "LoadRatingsParquet",
    input_location        = Seq(job_props.ratings_input_path),
    input_type            = CSV(),
    transform_with_state  = enrichRatingData(spark, job_props),
    output_type           = CSV(),
    output_location       = job_props.ratings_output_path,
    output_partition_col  = Seq(f"$temp_date_col"),
    output_save_mode      = SaveMode.Overwrite
  )(spark)

  val step2 = BQLoadStep[RatingOutput](
    name           = "LoadRatingBQ",
    input_location = Right(output_date_paths),
    input_type     = CSV(),
    output_dataset = job_props.ratings_output_dataset,
    output_table   = job_props.ratings_output_table_name
  )(bq)

  val etl_step_list: List[StateLessEtlStep] = EtlStepList(step1, step2)
}
