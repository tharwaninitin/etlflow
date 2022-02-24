package examples

import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.Environment.LOCAL
import etlflow.spark.{IOType, SparkImpl, SparkManager}
import etlflow.utils.ApplicationLogger
import Globals.{default_ratings_input_path, default_ratings_output_path}
import examples.Schema.Rating
import org.apache.spark.sql.SaveMode
import zio.{ExitCode, UIO, URIO}

object EtlJobParquetToOrc extends zio.App with ApplicationLogger {

  private lazy val spark = SparkManager.createSparkSession(Set(LOCAL), hive_support = false)

  private val step1 = SparkReadWriteStep[Rating, Rating](
    name = "LoadRatingsParquet",
    input_location = Seq(default_ratings_input_path),
    input_type = IOType.PARQUET,
    output_type = IOType.ORC,
    output_save_mode = SaveMode.Overwrite,
    output_location = default_ratings_output_path,
    output_repartitioning = true,
    output_repartitioning_num = 1,
    output_filename = Some("ratings.orc")
  )

  val job = step1.execute.provideLayer(SparkImpl.live(spark) ++ etlflow.log.nolog)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
