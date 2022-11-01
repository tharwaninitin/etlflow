package etlflow.task

import etlflow.SparkTestSuiteHelper
import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.schema.Rating
import etlflow.spark.{IOType, SparkEnv}
import org.apache.spark.sql.SaveMode
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

object ParquetToJsonTestSuite extends ApplicationLogger with SparkTestSuiteHelper {

  // Note: Here Parquet file has 6 columns and Rating Case Class has 4 out of those 6 columns so only 4 will be selected
  val task1: RIO[SparkEnv with Audit, Unit] = SparkReadWriteTask[Rating, Rating](
    name = "LoadRatingsParquetToJdbc",
    inputLocation = List(inputPathParquet),
    inputType = IOType.PARQUET,
    outputType = IOType.JSON(),
    outputLocation = outputPath,
    outputSaveMode = SaveMode.Overwrite,
    outputRepartitioning = true,
    outputRepartitioningNum = 1,
    outputFilename = Some("ratings.json")
  ).execute

  val spec: Spec[SparkEnv with Audit, Any] =
    test("ParquetToJsonTestSuite task should run successfully")(
      assertZIO(task1.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Ok")))(equalTo("Ok"))
    )
}
