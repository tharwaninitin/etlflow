package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.config.typesafe.TypesafeConfigSource
import zio.test.Assertion.equalTo
import zio.test._
import zio.config._

object K8SDeleteJobTestSuite {
  private lazy val config = K8SDeleteJobTask(
    name = "Test",
    jobName = "K8S Job"
  )

  private lazy val json =
    """
      |{
      | "name" : "Test",
      | "jobName": "K8S Job"
      |}
      |""".stripMargin

  val configSpec: Spec[Any, Any] =
    test("Parse TrackKubeJobTask") {
      val source = TypesafeConfigSource.fromHoconString(json)
      val parsed = read(K8SDeleteJobTask.config.from(source))
      assertZIO(parsed)(equalTo(config))
    }
  val spec: Spec[K8S with Audit, Any] =
    test("Execute TrackKubeJobTask") {
      val task = K8SDeleteJobTask("DeleteKubeJobTask", jobName = jobName).toZIO
      assertZIO(
        task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))
      )(equalTo("ok"))
    }
}
