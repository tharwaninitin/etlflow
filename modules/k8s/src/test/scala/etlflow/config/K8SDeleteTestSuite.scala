package etlflow.config

import etlflow.task.K8SDeleteJobTask
import zio.config._
import zio.config.typesafe.TypesafeConfigSource
import zio.test.Assertion.equalTo
import zio.test._

object K8SDeleteTestSuite {
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
}
