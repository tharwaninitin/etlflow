package etlflow.task

import zio.config._
import zio.config.typesafe.TypesafeConfigSource
import zio.test.Assertion._
import zio.test._

object DPSparkJobTaskTestSuite {

  private lazy val config = DPSparkJobTask(
    name = "Test",
    args = List("1000"),
    mainClass = "com.example.Main",
    libs = List("/path/to/jar"),
    conf = Map("k" -> "v")
  )

  private lazy val json =
    """
      |{
      | "name": "Test",
      | "args": [
      |   "1000"
      | ],
      | "mainClass": "com.example.Main",
      | "libs": [
      |   "/path/to/jar"
      | ],
      | "conf": {
      |   "k": "v"
      | }
      |}
      |""".stripMargin

  val parseSpec: Spec[Any, Any] =
    test("Parse Spark Job") {
      val source = TypesafeConfigSource.fromHoconString(json)
      val parsed = read(DPSparkJobTask.config.from(source))
      assertZIO(parsed)(equalTo(config))
    }

}
