package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DeleteKubeJobTestSuite {
  val spec: Spec[K8S with Audit, Any] =
    test("Execute TrackKubeJobTask") {
      val task = DeleteKubeJobTask("DeleteKubeJobTask", jobName = jobName, debug = true).execute
      assertZIO(
        task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))
      )(equalTo("ok"))
    }
}
