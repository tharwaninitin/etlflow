package etlflow.steps

import etlflow.TestHelper
import etlflow.etlsteps.DPDeleteStep
import etlflow.log.LogEnv
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DPDeleteTestSuite extends TestHelper {
  val spec: ZSpec[environment.TestEnvironment with DPEnv with LogEnv, Any] =
    testM("Execute DPDeleteStep") {
      val step = DPDeleteStep(
        name = "DPDeleteStepExample",
        dp_cluster_name,
        gcp_project_id.get,
        gcp_region.get
      ).execute
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
