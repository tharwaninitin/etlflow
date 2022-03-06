package etlflow.etlsteps

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
  val step1: RIO[SparkEnv with LogEnv, Unit] = SparkReadWriteStep[Rating, Rating](
    name = "LoadRatingsParquetToJdbc",
    input_location = Seq(input_path_parquet),
    input_type = IOType.PARQUET,
    output_type = IOType.JSON(),
    output_location = output_path,
    output_save_mode = SaveMode.Overwrite,
    output_repartitioning = true,
    output_repartitioning_num = 1,
    output_filename = Some("ratings.json")
  ).execute

  val test: ZSpec[environment.TestEnvironment with SparkEnv with LogEnv, Any] =
    testM("ParquetToJsonTestSuite step should run successfully")(
      assertM(step1.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Ok")))(equalTo("Ok"))
    )
}
