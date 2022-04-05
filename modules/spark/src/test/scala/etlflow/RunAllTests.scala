package etlflow

import etlflow.spark.SparkImpl
import etlflow.task._
import zio.test._

object RunAllTests extends DefaultRunnableSpec with TestSparkSession {
  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Spark Steps")(
      ParquetToJsonTestSuite.test,
      SparkDeDupTestSuite.test,
      ParquetToJdbcTestSuite.test,
      ParquetToJdbcGenericTestSuite.test,
      TransformationTestSuite.spec,
      BQtoGCStoGCSTestSuite.test @@ TestAspect.ignore
    ).provideCustomLayerShared((SparkImpl.live(spark) ++ log.noLog).orDie) @@ TestAspect.sequential
}
