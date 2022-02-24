package examples

import etlflow.etlsteps.{SparkReadWriteStep, SparkStep}
import etlflow.spark.Environment.{GCP, LOCAL}
import etlflow.spark.{IOType, ReadApi, SparkImpl, SparkManager}
import etlflow.utils.ApplicationLogger
import Globals.default_ratings_input_path_csv
import examples.Schema.{Rating, RatingOutput}
import org.apache.spark.sql.functions.{col, from_unixtime, input_file_name, unix_timestamp}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql._
import zio.{ExitCode, URIO}

object EtlJobCsvToParquetGcs extends zio.App with ApplicationLogger {

  private val gcs_output_path                          = f"gs://${sys.env("GCS_BUCKET")}/output/ratings1"
  private var output_date_paths: Seq[(String, String)] = Seq()
  private val temp_date_col                            = "temp_date_col"

  implicit private val spark: SparkSession = SparkManager.createSparkSession(
    Set(LOCAL, GCP(sys.env("GOOGLE_APPLICATION_CREDENTIALS"), sys.env("GCP_PROJECT_ID"))),
    hive_support = false
  )

  val get_formatted_date: (String, String, String) => Column =
    (ColumnName: String, ExistingFormat: String, NewFormat: String) =>
      from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)

  def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {
    import spark.implicits._
    // val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(temp_date_col, get_formatted_date("date", "yyyy-MM-dd", "yyyyMMdd"))
      .where(f"$temp_date_col in ('20160101', '20160102')")

    ratings_df.as[RatingOutput]
  }

  def addFilePaths()(spark: SparkSession): Unit = {
    import spark.implicits._
    output_date_paths = ReadApi
      .DS[RatingOutput](Seq(gcs_output_path), IOType.PARQUET)(spark)
      .select(f"$temp_date_col")
      .withColumn("filename", input_file_name)
      .distinct()
      .as[(String, String)]
      .collect()
      .map(date => (gcs_output_path + f"/$temp_date_col=" + date._1 + "/" + date._2.split("/").last, date._1))

    logger.info("Filepaths generated are: ")
    output_date_paths.foreach(path => println(path))
  }

  private val step1 = SparkReadWriteStep[Rating, RatingOutput](
    name = "LoadRatingsParquet",
    input_location = Seq(default_ratings_input_path_csv),
    input_type = IOType.CSV(",", true, "FAILFAST"),
    transform_function = Some(enrichRatingData),
    output_type = IOType.PARQUET,
    output_location = gcs_output_path,
    output_save_mode = SaveMode.Overwrite,
    output_partition_col = Seq(f"$temp_date_col"),
    output_repartitioning = true
  )

  private val step2 = SparkStep(
    name = "GenerateFilePaths",
    transform_function = addFilePaths()
  )

  val job = for {
    _ <- step1.execute
    _ <- step2.execute
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    job.provideLayer(SparkImpl.live(spark) ++ etlflow.log.nolog).exitCode
}
