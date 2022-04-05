package etlflow.steps

import etlflow.TestHelper
import etlflow.log.LogEnv
import etlflow.task.DPCreateTask
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object DPCreateTestSuite extends TestHelper {
  val spec: ZSpec[environment.TestEnvironment with DPEnv with LogEnv, Any] =
    testM("Execute DPCreateStep") {
      val dpProps = DataprocProperties(
        bucket_name = dpBucket,
        subnet_uri = dpSubnetUri,
        network_tags = dpNetworkTags,
        service_account = dpServiceAccount
      )
      val step = DPCreateTask("DPCreateStepExample", dpCluster, gcpProjectId.get, gcpRegion.get, dpProps).executeZio
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
