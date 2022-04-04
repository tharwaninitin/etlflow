package examples

import etlflow.spark.Environment.{GCP, LOCAL}
import etlflow.spark.{IOType, SparkImpl, SparkManager}
import etlflow.utils.ApplicationLogger
import Globals.defaultRatingsInputPathCsv
import etlflow.etltask.SparkReadWriteTask
import examples.Schema.{Rating, RatingOutput}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql._
import zio.{ExitCode, URIO}

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.NonUnitStatements"))
object EtlJobCsvToCsvGcs extends zio.App with ApplicationLogger {

  private val gcsOutputPath                  = f"gs://${sys.env("GCS_BUCKET")}/output/ratings1"
  var outputDatePaths: Seq[(String, String)] = Seq()
  private val tempDateCol                    = "temp_date_col"

  implicit private val spark: SparkSession = SparkManager.createSparkSession(
    Set(LOCAL, GCP(sys.env("GOOGLE_APPLICATION_CREDENTIALS"), sys.env("GCP_PROJECT_ID"))),
    hiveSupport = false
  )

  val getFormattedDate: (String, String, String) => Column =
    (ColumnName: String, ExistingFormat: String, NewFormat: String) =>
      from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)

  private def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {

    import spark.implicits._

    val ratingsDf = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(tempDateCol, getFormattedDate("date", "yyyy-MM-dd", "yyyyMMdd"))
      .where(f"$tempDateCol in ('20160101', '20160102')")

    outputDatePaths = ratingsDf
      .select(f"$tempDateCol")
      .distinct()
      .as[String]
      .collect()
      .map(date => (gcsOutputPath + f"/$tempDateCol=" + date + "/part*", date))

    ratingsDf.drop(f"$tempDateCol")

    val mapping = Encoders.product[RatingOutput]
    ratingsDf.as[RatingOutput](mapping)
  }

  private val step1 = SparkReadWriteTask[Rating, RatingOutput](
    name = "LoadRatingsParquet",
    inputLocation = List(defaultRatingsInputPathCsv),
    inputType = IOType.CSV(),
    transformFunction = Some(enrichRatingData),
    outputType = IOType.CSV(),
    outputLocation = gcsOutputPath,
    outputPartitionCol = Seq(f"$tempDateCol"),
    outputSaveMode = SaveMode.Overwrite
  ).executeZio.provideLayer(SparkImpl.live(spark) ++ etlflow.log.noLog)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = step1.exitCode
}
