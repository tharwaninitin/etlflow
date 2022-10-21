package etlflow.task

import etlflow.audit
import etlflow.k8s.K8s
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object CreateK8sJobTestSuite {
  private val env = K8s.live ++ audit.noLog

  val spec: Spec[Any, String] = suite("K8s Tasks - 1")(
    test("Execute CreateK8sJobTask") {
      val task = CreateK8sJobTask(
        name = "hello",
        image = "busybox:1.28",
        command = Some(Vector("/bin/sh", "-c", "date; echo Hello from the Kubernetes cluster"))
      ).execute.provide(env.orDie)
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
  )
}
