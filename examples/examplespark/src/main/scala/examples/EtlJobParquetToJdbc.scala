package examples

import etlflow.task.SparkReadWriteTask
import etlflow.model.Credential.JDBC
import etlflow.spark.IOType.RDB
import etlflow.spark.{IOType, SparkImpl, SparkManager}
import etlflow.utils.ApplicationLogger
import examples.Globals.defaultRatingsInputPath
import examples.Schema.Rating
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.{ExitCode, URIO}

object EtlJobParquetToJdbc extends zio.App with ApplicationLogger {

  private lazy val spark: SparkSession =
    SparkManager.createSparkSession(Set(etlflow.spark.Environment.LOCAL), hiveSupport = false)

  private val task1 = SparkReadWriteTask[Rating, Rating](
    name = "LoadRatingsParquetToJdbc",
    inputLocation = List(defaultRatingsInputPath),
    inputType = IOType.PARQUET,
    outputType = RDB(JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))),
    outputLocation = "ratings",
    outputSaveMode = SaveMode.Overwrite
  )

  private val job = task1.execute.provideLayer(SparkImpl.live(spark) ++ etlflow.log.noLog)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
