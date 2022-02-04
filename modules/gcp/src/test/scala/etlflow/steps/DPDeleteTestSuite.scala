package etlflow.steps

import etlflow.TestHelper
import etlflow.etlsteps.DPDeleteStep
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DPDeleteTestSuite extends TestHelper {
  val spec: ZSpec[environment.TestEnvironment with DPEnv, Any] =
    testM("Execute DPDeleteStep") {
      val step = DPDeleteStep(
        name = "DPDeleteStepExample",
        dp_cluster_name,
        gcp_project_id.get,
        gcp_region.get
      ).process
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
