package etlflow

import etlflow.steps._
import gcp4zio._
import zio.test._

object RunTests extends DefaultRunnableSpec with TestHelper {

  private val env = DPJob.live(dpEndpoint) ++ DP.live(dpEndpoint) ++ BQ.live() ++ GCS.live() ++ log.noLog

  override def spec: ZSpec[environment.TestEnvironment, Any] = (suite("GCP Steps")(
    BQStepsTestSuite.spec,
    GCSStepsTestSuite.spec,
    DPCreateTestSuite.spec,
    DPStepsTestSuite.spec,
    DPDeleteTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomLayerShared(env.orDie)
}
