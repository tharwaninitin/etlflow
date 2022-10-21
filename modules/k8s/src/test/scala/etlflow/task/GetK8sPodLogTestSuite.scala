package etlflow.task

import com.coralogix.zio.k8s.client.v1.pods.Pods
import etlflow.audit.AuditEnv
import etlflow.k8s.K8sEnv
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object GetK8sPodLogTestSuite {
  val spec: Spec[K8sEnv with Pods with AuditEnv, Any] =
    test("Execute GetK8sPodLogTask") {
      val task = GetK8sPodLogTask(name = "K8sJobTaskExample").execute
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
