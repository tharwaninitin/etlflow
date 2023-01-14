package etlflow

import etlflow.spark.SparkLive
import etlflow.task._
import zio.test.{ZIOSpecDefault, _}

object RunAllTests extends ZIOSpecDefault with TestSparkSession {
  override def spec: Spec[TestEnvironment, Any] =
    suite("Spark Tasks")(
      ParquetToJsonTestSuite.spec,
      SparkDeDupTestSuite.spec,
      ParquetToJdbcTestSuite.spec,
      ParquetToJdbcGenericTestSuite.spec,
      TransformationTestSuite.spec
      // BQtoGCStoGCSTestSuite.spec @@ TestAspect.ignore
    ).provideShared((SparkLive.live(spark) ++ audit.noop).orDie) @@ TestAspect.sequential
}
