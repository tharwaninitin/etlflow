package etlflow.task

import etlflow.TestSparkSession
import etlflow.audit.LogEnv
import etlflow.schema.Rating
import etlflow.spark.IOType.CSV
import etlflow.spark.SparkEnv
import org.apache.spark.sql.functions.{current_timestamp, lit}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object SparkDeDupTestSuite extends TestSparkSession {

  private val task = SparkDeDupTask[Rating](
    name = "LoadCsv",
    inputType = CSV(),
    inputLocation = "modules/spark/src/test/resources/input/ratings/*",
    transformation = _.withColumn("watermark_ts", lit(current_timestamp())),
    checkpointLocation = s"modules/spark/src/test/resources/checkpoint",
    eventTimeCol = "watermark_ts",
    delayThreshold = "10 days",
    deDupCols = Seq("user_id", "movie_id")
  )

  val spec: Spec[TestEnvironment with SparkEnv with LogEnv, Any] =
    test("Spark Streaming Deduplication task should execute successfully")(
      assertZIO(task.execute.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    )
}
