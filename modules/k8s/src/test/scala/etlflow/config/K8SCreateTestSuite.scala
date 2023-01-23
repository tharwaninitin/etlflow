package etlflow.config

import etlflow.task.K8SJobTask
import zio.config._
import zio.config.typesafe.TypesafeConfigSource
import zio.test.Assertion.equalTo
import zio.test._

object K8SCreateTestSuite {
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

  val configSpec: Spec[Any, Any] = test("Parse CreateKubeJobTask") {
    val source = TypesafeConfigSource.fromHoconString(json)
    val parsed = read(K8SJobTask.config.from(source))
    assertZIO(parsed)(equalTo(config))
  }
}
