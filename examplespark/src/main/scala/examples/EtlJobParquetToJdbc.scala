package examples

import etlflow.etlsteps.SparkReadWriteStep
import etlflow.schema.Credential.JDBC
import etlflow.spark.IOType.RDB
import etlflow.spark.{IOType, SparkManager}
import etlflow.utils.ApplicationLogger
import examples.Globals.default_ratings_input_path
import examples.Schema.Rating
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.{ExitCode, UIO, URIO}

object EtlJobParquetToJdbc extends zio.App with ApplicationLogger {

  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(etlflow.spark.Environment.LOCAL), hive_support = false)

  private val step1 = SparkReadWriteStep[Rating](
    name = "LoadRatingsParquetToJdbc",
    input_location = List(default_ratings_input_path),
    input_type = IOType.PARQUET,
    output_type = RDB(JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))),
    output_location = "ratings",
    output_save_mode = SaveMode.Overwrite
  )

  val job = step1.process(()) *> UIO(spark.stop())

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
