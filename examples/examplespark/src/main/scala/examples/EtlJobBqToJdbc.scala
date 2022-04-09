package examples

import etlflow.task.SparkReadWriteTask
import etlflow.model.Credential.JDBC
import etlflow.spark.IOType.RDB
import etlflow.spark.{IOType, SparkImpl, SparkManager}
import etlflow.utils.ApplicationLogger
import examples.Schema.RatingBQ
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.{ExitCode, URIO}

object EtlJobBqToJdbc extends zio.App with ApplicationLogger {

  implicit private val spark: SparkSession =
    SparkManager.createSparkSession(Set(etlflow.spark.Environment.LOCAL), hiveSupport = false)

  private val task1 = SparkReadWriteTask[RatingBQ, RatingBQ](
    name = "LoadRatingsBqToJdbc",
    inputLocation = List("dev.ratings"),
    inputType = IOType.BQ(),
    outputType = RDB(JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))),
    outputLocation = "ratings",
    outputSaveMode = SaveMode.Overwrite
  ).execute.provideLayer(SparkImpl.live(spark) ++ etlflow.log.noLog)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = task1.exitCode
}
