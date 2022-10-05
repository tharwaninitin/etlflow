package examples

import etlflow.spark.Environment.{GCP, LOCAL}
import etlflow.spark.{IOType, ReadApi, SparkLive, SparkManager}
import etlflow.task.{SparkReadWriteTask, SparkTask}
import etlflow.utils.ApplicationLogger
import examples.Globals.defaultRatingsInputPathCsv
import examples.Schema.{Rating, RatingOutput}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_unixtime, input_file_name, unix_timestamp}
import org.apache.spark.sql.types.DateType
import zio.Task

@SuppressWarnings(Array("org.wartremover.warts.Var"))
object EtlJobCsvToParquetGcs extends zio.ZIOAppDefault with ApplicationLogger {

  private val gcsOutputPath                          = f"gs://${sys.env("GCS_BUCKET")}/output/ratings1"
  private var outputDatePaths: Seq[(String, String)] = Seq()
  private val tempDateCol                            = "temp_date_col"

  val spark: SparkSession = SparkManager.createSparkSession(
    Set(LOCAL, GCP(sys.env("GOOGLE_APPLICATION_CREDENTIALS"), sys.env("GCP_PROJECT_ID"))),
    hiveSupport = false
  )

  val getFormattedDate: (String, String, String) => Column =
    (ColumnName: String, ExistingFormat: String, NewFormat: String) =>
      from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)

  def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {
    import spark.implicits._
    // val mapping = Encoders.product[RatingOutput]

    val ratingsDf = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(tempDateCol, getFormattedDate("date", "yyyy-MM-dd", "yyyyMMdd"))
      .where(f"$tempDateCol in ('20160101', '20160102')")

    ratingsDf.as[RatingOutput]
  }

  def addFilePaths()(spark: SparkSession): Unit = {
    import spark.implicits._
    outputDatePaths = ReadApi
      .ds[RatingOutput](List(gcsOutputPath), IOType.PARQUET)(spark)
      .select(f"$tempDateCol")
      .withColumn("filename", input_file_name)
      .distinct()
      .as[(String, String)]
      .collect()
      .map(date => (gcsOutputPath + f"/$tempDateCol=" + date._1 + "/" + date._2.split("/").last, date._1))

    logger.info("Filepaths generated are: ")
    outputDatePaths.foreach(path => println(path))
  }

  private val task1 = SparkReadWriteTask[Rating, RatingOutput](
    name = "LoadRatingsParquet",
    inputLocation = List(defaultRatingsInputPathCsv),
    inputType = IOType.CSV(",", true, "FAILFAST"),
    transformFunction = Some(enrichRatingData),
    outputType = IOType.PARQUET,
    outputLocation = gcsOutputPath,
    outputSaveMode = SaveMode.Overwrite,
    outputPartitionCol = Seq(f"$tempDateCol"),
    outputRepartitioning = true
  )

  private val task2 = SparkTask(name = "GenerateFilePaths", transformFunction = addFilePaths())

  private val job = for {
    _ <- task1.execute
    _ <- task2.execute
  } yield ()

  override def run: Task[Unit] = job.provideLayer(SparkLive.live(spark) ++ etlflow.audit.noLog)
}
