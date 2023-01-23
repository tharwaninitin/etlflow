package etlflow.task

import zio.config._
import zio.config.typesafe.TypesafeConfigSource
import zio.test.Assertion._
import zio.test._

object BQQueryTaskTestSuite {

  private lazy val config = BQQueryTask(
    name = "Test",
    queries = List("SELECT * FROM dual")
  )

  private lazy val json =
    """
      |{
      | "name": "Test",
      | "queries": [
      |   "SELECT * FROM dual"
      | ]
      |}
      |""".stripMargin

  val parseSpec: Spec[Any, Any] =
    test("Parse BQ Query") {
      val source = TypesafeConfigSource.fromHoconString(json)
      val parsed = read(BQQueryTask.config.from(source))
      assertZIO(parsed)(equalTo(config))
    }

}
