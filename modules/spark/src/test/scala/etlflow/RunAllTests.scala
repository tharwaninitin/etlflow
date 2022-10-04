package etlflow

import etlflow.spark.SparkLive
import etlflow.task._
import zio.test._
import zio.test.ZIOSpecDefault

object RunAllTests extends ZIOSpecDefault with TestSparkSession {
  override def spec: Spec[TestEnvironment, Any] =
    suite("Spark Tasks")(
      ParquetToJsonTestSuite.spec,
      SparkDeDupTestSuite.spec,
      ParquetToJdbcTestSuite.spec,
      ParquetToJdbcGenericTestSuite.spec,
      TransformationTestSuite.spec
      // BQtoGCStoGCSTestSuite.spec @@ TestAspect.ignore
    ).provideCustomShared((SparkLive.live(spark) ++ audit.noLog).orDie) @@ TestAspect.sequential
}
