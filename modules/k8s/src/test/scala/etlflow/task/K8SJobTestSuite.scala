package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object K8SJobTestSuite {

  val spec: Spec[K8S with Audit, Any] = test("Execute CreateKubeJobTask") {
    val task = K8SJobTask(
      name = "name",
      jobName = jobName,
      image = "alpine:latest",
      command = Some(List("/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster")),
      volumeMounts = Some(Map("/etc/svc-wmt-cill-dev" -> "svc-wmt-cill-dev"))
    ).toZIO
    assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
  }
}
