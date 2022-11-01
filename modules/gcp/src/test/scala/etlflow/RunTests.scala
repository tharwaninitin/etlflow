package etlflow

import etlflow.task._
import gcp4zio.bq.BQ
import gcp4zio.dp.{DPCluster, DPJob}
import gcp4zio.gcs.GCS
import zio.Clock.ClockLive
import zio.ZLayer
import zio.test._

object RunTests extends ZIOSpecDefault with TestHelper {

  private val env =
    DPJob.live(dpEndpoint) ++ DPCluster.live(dpEndpoint) ++ BQ.live() ++ GCS.live() ++ audit.test ++ ZLayer.succeed(ClockLive)

  override def spec: Spec[TestEnvironment, Any] = (suite("GCP Tasks")(
    BQTestSuite.spec,
    GCSTasksTestSuite.spec,
    DPCreateTestSuite.spec,
    DPTasksTestSuite.spec,
    DPDeleteTestSuite.spec
  ) @@ TestAspect.sequential).provideShared(env.orDie)
}
