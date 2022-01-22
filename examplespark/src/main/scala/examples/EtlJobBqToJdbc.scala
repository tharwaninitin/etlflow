package examples

import etlflow.etlsteps.SparkReadWriteStep
import etlflow.model.Credential.JDBC
import etlflow.spark.IOType.RDB
import etlflow.spark.{IOType, SparkManager}
import etlflow.utils.ApplicationLogger
import examples.Schema.RatingBQ
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.{ExitCode, URIO}

object EtlJobBqToJdbc extends zio.App with ApplicationLogger {

  implicit private val spark: SparkSession =
    SparkManager.createSparkSession(Set(etlflow.spark.Environment.LOCAL), hive_support = false)

  private val step1 = SparkReadWriteStep[RatingBQ](
    name = "LoadRatingsBqToJdbc",
    input_location = Seq("dev.ratings"),
    input_type = IOType.BQ(),
    output_type = RDB(JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))),
    output_location = "ratings",
    output_save_mode = SaveMode.Overwrite
  )

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = step1.process.exitCode
}
