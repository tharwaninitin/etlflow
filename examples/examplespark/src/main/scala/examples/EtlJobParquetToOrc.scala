package examples

import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.Environment.LOCAL
import etlflow.spark.{IOType, SparkImpl, SparkManager}
import etlflow.utils.ApplicationLogger
import Globals.{defaultRatingsInputPath, defaultRatingsOutputPath}
import examples.Schema.Rating
import org.apache.spark.sql.SaveMode
import zio.{ExitCode, UIO, URIO}

object EtlJobParquetToOrc extends zio.App with ApplicationLogger {

  private lazy val spark = SparkManager.createSparkSession(Set(LOCAL), hiveSupport = false)

  private val step1 = SparkReadWriteStep[Rating, Rating](
    name = "LoadRatingsParquet",
    inputLocation = List(defaultRatingsInputPath),
    inputType = IOType.PARQUET,
    outputType = IOType.ORC,
    outputSaveMode = SaveMode.Overwrite,
    outputLocation = defaultRatingsOutputPath,
    outputRepartitioning = true,
    outputRepartitioningNum = 1,
    outputFilename = Some("ratings.orc")
  )

  private val job = step1.execute.provideLayer(SparkImpl.live(spark) ++ etlflow.log.noLog)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
