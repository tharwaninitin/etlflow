package etlflow

import etlflow.task._
import gcp4zio._
import zio.test._

object RunTests extends DefaultRunnableSpec with TestHelper {

  private val env = DPJob.live(dpEndpoint) ++ DP.live(dpEndpoint) ++ BQ.live() ++ GCS.live() ++ log.noLog

  override def spec: ZSpec[environment.TestEnvironment, Any] = (suite("GCP Tasks")(
    BQTestSuite.spec,
    GCSTasksTestSuite.spec,
    DPCreateTestSuite.spec,
    DPTasksTestSuite.spec,
    DPDeleteTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomLayerShared(env.orDie)
}
