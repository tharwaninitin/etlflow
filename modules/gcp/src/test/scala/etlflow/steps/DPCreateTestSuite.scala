package etlflow.steps

import etlflow.TestHelper
import etlflow.etlsteps.DPCreateStep
import etlflow.log.LogEnv
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DPCreateTestSuite extends TestHelper {
  val spec: ZSpec[environment.TestEnvironment with DPEnv with LogEnv, Any] =
    testM("Execute DPCreateStep") {
      val dpProps = DataprocProperties(
        bucket_name = dp_bucket_name,
        subnet_uri = dp_subnet_uri,
        network_tags = dp_network_tags,
        service_account = dp_service_account
      )
      val step = DPCreateStep("DPCreateStepExample", dp_cluster_name, gcp_project_id.get, gcp_region.get, dpProps).execute
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
