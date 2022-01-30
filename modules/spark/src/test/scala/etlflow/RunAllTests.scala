package etlflow

import etlflow.etlsteps._
import etlflow.spark.SparkImpl
import zio.test._

object RunAllTests extends DefaultRunnableSpec with TestSparkSession {
  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("All Spark Steps")(
      ParquetToJdbcTestSuite.spec,
      ParquetToJdbcGenericSparkStepTestSuite.spec,
      ParquetToJsonTestSuite.spec,
      TransformationTestSuite.spec,
      SparkDeDupTestSuite.spec
      // BQtoGCStoGCSTestSuite.spec,
    ).provideCustomLayerShared(SparkImpl.live(spark).orDie) @@ TestAspect.sequential
}
