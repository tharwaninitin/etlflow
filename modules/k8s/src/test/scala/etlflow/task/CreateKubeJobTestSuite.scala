package etlflow.task

import etlflow.audit.AuditEnv
import etlflow.k8s.K8sEnv
import zio.test.Assertion.equalTo
import zio.test._
import zio.ZIO

object CreateKubeJobTestSuite {

  val spec: Spec[K8sEnv with AuditEnv, Any] = test("Execute CreateKubeJobTask") {
    val task = CreateKubeJobTask(
      name = "KubeJobTaskExample",
      image = "busybox:1.28",
      command = Some(Vector("/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster"))
    ).execute
    assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
  }
}
