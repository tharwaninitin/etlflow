package examples

import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.Environment.{GCP, LOCAL}
import etlflow.spark.{IOType, SparkImpl, SparkManager}
import etlflow.utils.ApplicationLogger
import Globals.default_ratings_input_path_csv
import examples.Schema.{Rating, RatingOutput}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql._
import zio.{ExitCode, URIO}

object EtlJobCsvToCsvGcs extends zio.App with ApplicationLogger {

  private val gcs_output_path                  = f"gs://${sys.env("GCS_BUCKET")}/output/ratings1"
  var output_date_paths: Seq[(String, String)] = Seq()
  private val temp_date_col                    = "temp_date_col"

  implicit private val spark: SparkSession = SparkManager.createSparkSession(
    Set(LOCAL, GCP(sys.env("GOOGLE_APPLICATION_CREDENTIALS"), sys.env("GCP_PROJECT_ID"))),
    hive_support = false
  )

  val get_formatted_date: (String, String, String) => Column =
    (ColumnName: String, ExistingFormat: String, NewFormat: String) =>
      from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)

  private def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {

    import spark.implicits._

    val ratings_df = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(temp_date_col, get_formatted_date("date", "yyyy-MM-dd", "yyyyMMdd"))
      .where(f"$temp_date_col in ('20160101', '20160102')")

    output_date_paths = ratings_df
      .select(f"$temp_date_col")
      .distinct()
      .as[String]
      .collect()
      .map(date => (gcs_output_path + f"/$temp_date_col=" + date + "/part*", date))

    ratings_df.drop(f"$temp_date_col")

    val mapping    = Encoders.product[RatingOutput]
    val ratings_ds = ratings_df.as[RatingOutput](mapping)
    ratings_ds
  }

  private val step1 = SparkReadWriteStep[Rating, RatingOutput](
    name = "LoadRatingsParquet",
    input_location = Seq(default_ratings_input_path_csv),
    input_type = IOType.CSV(),
    transform_function = Some(enrichRatingData),
    output_type = IOType.CSV(),
    output_location = gcs_output_path,
    output_partition_col = Seq(f"$temp_date_col"),
    output_save_mode = SaveMode.Overwrite
  ).execute.provideLayer(SparkImpl.live(spark) ++ etlflow.log.nolog)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = step1.exitCode
}
