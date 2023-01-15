package etlflow

import etlflow.log.ApplicationLogger
import etlflow.task._
import gcp4zio.bq.BQ
import gcp4zio.dp.{DPCluster, DPJob}
import gcp4zio.gcs.GCS
import zio.Clock.ClockLive
import zio.test._
import zio.{ULayer, ZLayer}

object RunTests extends ZIOSpecDefault with TestHelper with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  private val dpJobEnv = DPJob.live(dpCluster, gcpProjectId.get, gcpRegion.get, dpEndpoint)

  private val dpClusterEnv = DPCluster.live(gcpProjectId.get, gcpRegion.get, dpEndpoint)

  private val env = dpJobEnv ++ dpClusterEnv ++ BQ.live() ++ GCS.live() ++ audit.noop ++ ZLayer.succeed(ClockLive)

  override def spec: Spec[TestEnvironment, Any] = (suite("GCP Tasks")(
    BQTestSuite.spec,
    GCSTasksTestSuite.spec,
    DPCreateTestSuite.spec,
    DPTasksTestSuite.spec,
    DPDeleteTestSuite.spec
  ) @@ TestAspect.sequential).provideShared(env.orDie)
}
