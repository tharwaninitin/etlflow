package etlflow.task

import etlflow.TestHelper
import etlflow.log.LogEnv
import gcp4zio.dp._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object DPDeleteTestSuite extends TestHelper {
  val spec: Spec[TestEnvironment with DPEnv with LogEnv, Any] =
    test("Execute DPDeleteTask") {
      val task = DPDeleteTask(
        name = "DPDeleteTaskExample",
        dpCluster,
        gcpProjectId.get,
        gcpRegion.get
      ).execute
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
