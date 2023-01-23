package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._
import zio.config._
import zio.config.typesafe.TypesafeConfigSource

object K8SJobTestSuite {
  private lazy val config = K8SJobTask(
    name = "test",
    jobName = "K8S Job",
    image = "image_url"
  )
  private lazy val json =
    """
      |{
      | "name": "test",
      | "jobName": "K8S Job",
      | "image": "image_url"
      |}
      |""".stripMargin
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
  val configSpec: Spec[Any, Any] = test("Parse CreateKubeJobTask") {
    val source = TypesafeConfigSource.fromHoconString(json)
    val parsed = read(K8SJobTask.config.from(source))
    assertZIO(parsed)(equalTo(config))
  }
}
