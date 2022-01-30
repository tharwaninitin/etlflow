package etlflow.etlsteps

import etlflow.TestSparkSession
import etlflow.schema.Rating
import etlflow.spark.IOType.CSV
import etlflow.spark.SparkEnv
import org.apache.spark.sql.functions.{current_timestamp, lit}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object SparkDeDupTestSuite extends TestSparkSession {

  val spec: ZSpec[environment.TestEnvironment with SparkEnv, Any] = {

    val step = SparkDeDupStep[Rating](
      name = "LoadCsv",
      input_type = CSV(),
      input_location = "modules/spark/src/test/resources/input/ratings/*",
      transformation = _.withColumn("watermark_ts", lit(current_timestamp())),
      checkpointLocation = s"modules/spark/src/test/resources/checkpoint",
      eventTimeCol = "watermark_ts",
      delayThreshold = "10 days",
      deDupCols = Seq("user_id", "movie_id")
    )

    suite("Spark Streaming Step")(
      testM("Execute Batch") {
        assertM(step.process.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
  }
}
