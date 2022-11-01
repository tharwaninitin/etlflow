package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8sEnv
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object CreateKubeJobTestSuite {

  val spec: Spec[K8sEnv with Audit, Any] = test("Execute CreateKubeJobTask") {
    val task = CreateKubeJobTask(
      name = "KubeJobTaskExample",
      image = "busybox:1.28",
      command = Some(Vector("/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster"))
    ).execute
    assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
  }
}
