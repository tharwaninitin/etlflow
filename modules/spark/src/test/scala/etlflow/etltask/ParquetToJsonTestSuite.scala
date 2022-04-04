package etlflow.etltask

import etlflow.SparkTestSuiteHelper
import etlflow.log.LogEnv
import etlflow.schema.Rating
import etlflow.spark.IOType
import etlflow.spark.SparkEnv
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql.SaveMode
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

object ParquetToJsonTestSuite extends ApplicationLogger with SparkTestSuiteHelper {

  // Note: Here Parquet file has 6 columns and Rating Case Class has 4 out of those 6 columns so only 4 will be selected
  val step1: RIO[SparkEnv with LogEnv, Unit] = SparkReadWriteTask[Rating, Rating](
    name = "LoadRatingsParquetToJdbc",
    inputLocation = List(inputPathParquet),
    inputType = IOType.PARQUET,
    outputType = IOType.JSON(),
    outputLocation = outputPath,
    outputSaveMode = SaveMode.Overwrite,
    outputRepartitioning = true,
    outputRepartitioningNum = 1,
    outputFilename = Some("ratings.json")
  ).executeZio

  val test: ZSpec[environment.TestEnvironment with SparkEnv with LogEnv, Any] =
    testM("ParquetToJsonTestSuite step should run successfully")(
      assertM(step1.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Ok")))(equalTo("Ok"))
    )
}
