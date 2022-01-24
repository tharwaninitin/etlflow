package examples

import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.Environment.LOCAL
import etlflow.spark.{IOType, SparkManager}
import etlflow.utils.ApplicationLogger
import Globals.{default_ratings_input_path, default_ratings_output_path}
import examples.Schema.Rating
import org.apache.spark.sql.SaveMode
import zio.{ExitCode, UIO, URIO}

object EtlJobParquetToOrc extends zio.App with ApplicationLogger {

  implicit private val spark = SparkManager.createSparkSession(Set(LOCAL), hive_support = false)

  private val step1 = SparkReadWriteStep[Rating](
    name = "LoadRatingsParquet",
    input_location = Seq(default_ratings_input_path),
    input_type = IOType.PARQUET,
    output_type = IOType.ORC,
    output_location = default_ratings_output_path,
    output_repartitioning = true,
    output_repartitioning_num = 1,
    output_save_mode = SaveMode.Overwrite,
    output_filename = Some("ratings.orc")
  )

  val job = step1.process *> UIO(spark.stop())

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
