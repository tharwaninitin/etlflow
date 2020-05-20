package examples.jobs

import etlflow.LoggerResource
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.{BQLoadStep, SparkReadTransformWriteStep}
import etlflow.spark.{SparkManager, SparkUDF}
import etlflow.utils.CSV
import examples.MyGlobalProperties
import examples.schema.MyEtlJobProps
import examples.schema.MyEtlJobProps.EtlJob23Props
import examples.schema.MyEtlJobSchema.{Rating, RatingOutput}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import zio.Task

case class EtlJob3Definition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends EtlJob with SparkUDF with SparkManager {
  lazy val spark: SparkSession = createSparkSession(global_properties)
  private val gcs_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings"
  private var output_date_paths: Seq[(String,String)] = Seq()
  private val temp_date_col = "temp_date_col"
  private val job_props = job_properties.asInstanceOf[EtlJob23Props]

  private def enrichRatingData(job_properties: EtlJob23Props)(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {

    val ratings_df = in
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
    ratings_ds
  }

  private val step1 = SparkReadTransformWriteStep[Rating, RatingOutput](
    name                  = "LoadRatingsParquet",
    input_location        = Seq(job_props.ratings_input_path),
    input_type            = CSV(),
    transform_function    = enrichRatingData(job_props),
    output_type           = CSV(),
    output_location       = gcs_output_path,
    output_partition_col  = Seq(f"$temp_date_col"),
    output_save_mode      = SaveMode.Overwrite
  )

  private val step2 = BQLoadStep[RatingOutput](
    name           = "LoadRatingBQ",
    input_location = Right(output_date_paths),
    input_type     = CSV(),
    output_dataset = job_props.ratings_output_dataset,
    output_table   = job_props.ratings_output_table_name
  )

  def etlJob(implicit resource: LoggerResource): Task[Unit] = for {
    - <- step1.execute(spark)
    _ <- step2.execute()
   } yield ()
}
