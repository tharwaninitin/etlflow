package etlflow.etlsteps

import etlflow.TestSparkSession
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.types._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object SparkDeDupTestSuite extends DefaultRunnableSpec with TestSparkSession {

  override def spec: ZSpec[environment.TestEnvironment, Any] = {

    val schema = new StructType()
      .add("userId", IntegerType)
      .add("movieId", IntegerType)
      .add("rating", DoubleType)
      .add("timestamp", LongType)
    val df = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv("modules/spark/src/test/resources/input/ratings/*")
      .withColumn("watermark_ts", lit(current_timestamp()))
    val step = SparkDeDupStep(
      "Test",
      df,
      s"modules/spark/src/test/resources/checkpoint",
      "watermark_ts",
      "10 days",
      Seq("userId", "movieId")
    )

    suite("Spark Steps")(
      testM("Execute Batch") {
        assertM(step.process.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Stop Spark Session") {
        spark.stop()
        assertTrue(1 == 1)
      }
    ) @@ TestAspect.sequential
  }
}
