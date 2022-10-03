package etlflow

import etlflow.task._
import gcp4zio.bq.BQLive
import gcp4zio.dp.{DPJobLive, DPLive}
import gcp4zio.gcs.GCSLive
import zio.Clock.ClockLive
import zio.ZLayer
import zio.test._
import zio.test.ZIOSpecDefault

object RunTests extends ZIOSpecDefault with TestHelper {

  private val env = DPJobLive(dpEndpoint) ++ DPLive(dpEndpoint) ++ BQLive() ++ GCSLive() ++ log.noLog ++ ZLayer.succeed(ClockLive)

  override def spec: Spec[TestEnvironment, Any] = (suite("GCP Tasks")(
    BQTestSuite.spec,
    GCSTasksTestSuite.spec,
    DPCreateTestSuite.spec,
    DPTasksTestSuite.spec,
    DPDeleteTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomLayerShared(env.orDie)
}
