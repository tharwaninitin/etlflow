package etlflow

import etlflow.spark.SparkImpl
import etlflow.task._
import zio.test._
import zio.test.ZIOSpecDefault

object RunAllTests extends ZIOSpecDefault with TestSparkSession {
  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Spark Tasks")(
      ParquetToJsonTestSuite.test,
      SparkDeDupTestSuite.test,
      ParquetToJdbcTestSuite.test,
      ParquetToJdbcGenericTestSuite.test,
      TransformationTestSuite.spec,
      BQtoGCStoGCSTestSuite.test @@ TestAspect.ignore
    ).provideCustomLayerShared((SparkImpl.live(spark) ++ log.noLog).orDie) @@ TestAspect.sequential
}
