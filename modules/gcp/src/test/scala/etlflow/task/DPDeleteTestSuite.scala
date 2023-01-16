package etlflow.task

import etlflow.TestHelper
import etlflow.audit.Audit
import gcp4zio.dp._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object DPDeleteTestSuite extends TestHelper {
  val spec: Spec[DPCluster with Audit, Any] =
    test("Execute DPDeleteTask") {
      val task = DPDeleteTask("DPDeleteTaskExample", dpCluster).toZIO
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
