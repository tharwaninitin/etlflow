package etlflow

import etlflow.etlsteps._
import etlflow.spark.SparkImpl
import zio.test._

object RunAllTests extends DefaultRunnableSpec with TestSparkSession {
  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("All Spark Steps")(
      // SparkDeDupTestSuite.spec,
      SparkExample1TestSuite.spec
      // SparkExample2TestSuite.spec,
      // SparkExample3TestSuite.spec,
      // TransformationTestSuite.spec
    ).provideCustomLayerShared(SparkImpl.live(spark).orDie) @@ TestAspect.sequential
}
