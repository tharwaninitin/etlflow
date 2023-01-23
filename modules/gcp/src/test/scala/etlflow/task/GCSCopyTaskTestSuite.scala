package etlflow.task

import etlflow.gcp.Location
import zio.config._
import zio.config.typesafe.TypesafeConfigSource
import zio.test.Assertion._
import zio.test._

object GCSCopyTaskTestSuite {

  private lazy val json =
    """
      |{
      | "name": "Test",
      | "input": {
      |   "local": {
      |     "path": "local"
      |   }
      | },
      | "inputRecursive": true,
      | "output": {
      |   "gcs":{
      |     "bucket": "bucket",
      |     "path": "path"
      |   }
      | },
      | "parallelism": 5
      |}
      |""".stripMargin
  private lazy val config = GCSCopyTask(
    name = "Test",
    input = Location.LOCAL("local"),
    inputRecursive = true,
    output = Location.GCS("bucket", "path"),
    parallelism = 5
  )
  val parseSpec: Spec[Any, Any] =
    test("Parse GCS Copy Step") {
      val source = TypesafeConfigSource.fromHoconString(json)
      val parsed = read(GCSCopyTask.config.from(source))
      assertZIO(parsed)(equalTo(config))
    }

}
