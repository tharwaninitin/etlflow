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

  private lazy val spark = SparkManager.createSparkSession(Set(LOCAL), hiveSupport = false)

  private val step1 = SparkReadWriteStep[Rating, Rating](
    name = "LoadRatingsParquet",
    inputLocation = List(default_ratings_input_path),
    inputType = IOType.PARQUET,
    outputType = IOType.ORC,
    outputSaveMode = SaveMode.Overwrite,
    outputLocation = default_ratings_output_path,
    outputRepartitioning = true,
    outputRepartitioningNum = 1,
    outputFilename = Some("ratings.orc")
  )

  private val job = step1.execute.provideLayer(SparkImpl.live(spark) ++ etlflow.log.noLog)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
