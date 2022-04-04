package etlflow.steps

import etlflow.TestHelper
import etlflow.etltask.DPDeleteTask
import etlflow.log.LogEnv
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object DPDeleteTestSuite extends TestHelper {
  val spec: ZSpec[environment.TestEnvironment with DPEnv with LogEnv, Any] =
    testM("Execute DPDeleteStep") {
      val step = DPDeleteTask(
        name = "DPDeleteStepExample",
        dpCluster,
        gcpProjectId.get,
        gcpRegion.get
      ).executeZio
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
