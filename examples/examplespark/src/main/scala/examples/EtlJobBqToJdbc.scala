package examples

import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import etlflow.spark.IOType.RDB
import etlflow.spark.{IOType, SparkLive, SparkManager}
import etlflow.task.SparkReadWriteTask
import examples.Schema.RatingBQ
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.Task

object EtlJobBqToJdbc extends zio.ZIOAppDefault with ApplicationLogger {

  val spark: SparkSession = SparkManager.createSparkSession(Set(etlflow.spark.Environment.LOCAL), hiveSupport = false)

  private val task1 = SparkReadWriteTask[RatingBQ, RatingBQ](
    name = "LoadRatingsBqToJdbc",
    inputLocation = List("dev.ratings"),
    inputType = IOType.BQ(),
    outputType = RDB(JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))),
    outputLocation = "ratings",
    outputSaveMode = SaveMode.Overwrite
  ).execute.provideLayer(SparkLive.live(spark) ++ etlflow.audit.noop)

  override def run: Task[Unit] = task1
}
