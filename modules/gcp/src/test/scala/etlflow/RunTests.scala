package etlflow

import etlflow.steps._
import gcp4zio._
import zio.test._

object RunTests extends DefaultRunnableSpec with TestHelper {

  val env = DPJob.live(dp_endpoint) ++ DP.live(dp_endpoint) ++ BQ.live() ++ GCS.live()

  override def spec: ZSpec[environment.TestEnvironment, Any] = (suite("GCP Steps")(
    BQStepsTestSuite.spec,
    GCSStepsTestSuite.spec,
    DPCreateTestSuite.spec,
    DPStepsTestSuite.spec,
    DPDeleteTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomLayerShared(env.orDie)
}
