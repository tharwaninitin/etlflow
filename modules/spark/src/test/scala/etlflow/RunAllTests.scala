package etlflow

import etlflow.etlsteps._
import etlflow.spark.SparkImpl
import zio.test._

object RunAllTests extends DefaultRunnableSpec with TestSparkSession {
  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("All Spark Steps")(
      ParquetToJdbcTestSuite.spec,
      ParquetToJsonTestSuite.spec,
      TransformationTestSuite.spec
      // SparkDeDupTestSuite.spec,
      // SparkExample2TestSuite.spec,
      // SparkExample3TestSuite.spec,
    ).provideCustomLayerShared(SparkImpl.live(spark).orDie) @@ TestAspect.sequential
}
