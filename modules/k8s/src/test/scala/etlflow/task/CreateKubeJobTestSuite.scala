package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object CreateKubeJobTestSuite {
  val spec: Spec[K8S with Audit, Any] = test("Execute CreateKubeJobTask") {
    val task = CreateKubeJobTask(
      name = "name",
      jobName = jobName,
      container = containerName,
      command = List("/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster"),
      image = "alpine:latest",
      volumeMounts = Map("/etc/svc-wmt-cill-dev" -> "svc-wmt-cill-dev")
    ).toZIO
    assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
  }
}
