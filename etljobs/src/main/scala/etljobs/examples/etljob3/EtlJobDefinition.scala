package etljobs.examples.etljob3

import etljobs.bigquery.BigQueryManager
import etljobs.etlsteps.{BQLoadStep, DatasetWithState, SparkReadTransformWriteStateStep, StateLessEtlStep}
import etljobs.examples.MyGlobalProperties
import etljobs.examples.schema.MyEtlJobProps
import etljobs.examples.schema.MyEtlJobSchema.{Rating, RatingOutput}
import etljobs.examples.schema.MyEtlJobProps.EtlJob23Props
import etljobs.spark.{SparkManager, SparkUDF}
import etljobs.utils.{CSV, GlobalProperties}
import etljobs.{EtlJob, EtlJobProps, EtlStepList}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

case class EtlJobDefinition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends EtlJob with SparkManager with SparkUDF with BigQueryManager {
  private val gcs_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings"
  private var output_date_paths: Seq[(String,String)] = Seq()
  private val temp_date_col = "temp_date_col"
  private val job_props:EtlJob23Props  = job_properties.asInstanceOf[EtlJob23Props]

  private def enrichRatingData(spark: SparkSession, job_properties: EtlJob23Props)(in : DatasetWithState[Rating,Unit]) : DatasetWithState[RatingOutput,Unit] = {

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
        .map(date => (gcs_output_path + f"/$temp_date_col=" + date + "/part*", date))

    ratings_df.drop(f"$temp_date_col")

    val mapping = Encoders.product[RatingOutput]
    val ratings_ds = ratings_df.as[RatingOutput](mapping)

    DatasetWithState(ratings_ds,())
  }

  private val step1 = SparkReadTransformWriteStateStep[Rating, Unit, RatingOutput, Unit](
    name                  = "LoadRatingsParquet",
    input_location        = Seq(job_props.ratings_input_path),
    input_type            = CSV(),
    transform_with_state  = enrichRatingData(spark, job_props),
    output_type           = CSV(),
    output_location       = gcs_output_path,
    output_partition_col  = Seq(f"$temp_date_col"),
    output_save_mode      = SaveMode.Overwrite
  )(spark)

  private val step2 = BQLoadStep[RatingOutput](
    name           = "LoadRatingBQ",
    input_location = Right(output_date_paths),
    input_type     = CSV(),
    output_dataset = job_props.ratings_output_dataset,
    output_table   = job_props.ratings_output_table_name
  )(bq)

  val etl_step_list: List[StateLessEtlStep] = EtlStepList(step1, step2)
}
