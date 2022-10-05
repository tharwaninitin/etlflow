package examples

import etlflow.spark.Environment.LOCAL
import etlflow.spark.{IOType, SparkLive, SparkManager}
import etlflow.task.SparkReadWriteTask
import etlflow.log.ApplicationLogger
import examples.Globals.{defaultRatingsInputPath, defaultRatingsOutputPath}
import examples.Schema.Rating
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.Task

object EtlJobParquetToOrc extends zio.ZIOAppDefault with ApplicationLogger {

  val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL), hiveSupport = false)

  private val task1 = SparkReadWriteTask[Rating, Rating](
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

  private val job = task1.execute.provideLayer(SparkLive.live(spark) ++ etlflow.audit.noLog)

  override def run: Task[Unit] = job
}
