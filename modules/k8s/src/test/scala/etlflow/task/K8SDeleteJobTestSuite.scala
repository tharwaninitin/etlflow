package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object K8SDeleteJobTestSuite {
  val spec: Spec[K8S with Audit, Any] =
    test("Execute TrackKubeJobTask") {
      val task = K8SDeleteJobTask("DeleteKubeJobTask", jobName = jobName, debug = true).toZIO
      assertZIO(
        task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))
      )(equalTo("ok"))
    }
}
