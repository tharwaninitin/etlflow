package etlflow

import etlflow.etlsteps._
import etlflow.spark.SparkImpl
import zio.test._

object RunAllTests extends DefaultRunnableSpec with TestSparkSession {
  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Spark Steps")(
      ParquetToJsonTestSuite.test,
      SparkDeDupTestSuite.test,
      ParquetToJdbcTestSuite.test,
      ParquetToJdbcGenericSparkStepTestSuite.test,
      TransformationTestSuite.spec,
      BQtoGCStoGCSTestSuite.test @@ TestAspect.ignore
    ).provideCustomLayerShared((SparkImpl.live(spark) ++ log.nolog).orDie) @@ TestAspect.sequential
}
