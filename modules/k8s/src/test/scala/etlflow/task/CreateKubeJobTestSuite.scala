package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import etlflow.audit.Audit
import etlflow.k8s.{K8S, Secret}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object CreateKubeJobTestSuite {
  val spec: Spec[K8S with Jobs with Audit, Any] = test("Execute CreateKubeJobTask") {
    val task = CreateKubeJobTask(
      name = "KubeJobTaskExample",
      image = "busybox:1.28",
      command = Some(Vector("/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster")),
      secret = Some(Secret("secret", "/etc/secret"))
    ).execute
    assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
  }
}
