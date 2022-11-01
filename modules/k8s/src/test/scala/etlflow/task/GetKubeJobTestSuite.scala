package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8sEnv
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object GetKubeJobTestSuite {
  val spec: Spec[K8sEnv with Audit, Any] =
    test("Execute GetKubeJobTask") {
      val task = GetKubeJobTask(name = "KubeJobTaskExample").execute
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
