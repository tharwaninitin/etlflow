package etlflow.json

import etlflow.task.DPSparkJobTask
import etlflow.task.DPSparkJobTask._
import zio.json._
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object DPSparkJobTaskJson {
  val spec: Spec[Any, Any] =
    test("Execute DPSparkJobTaskConfig") {
      val json = DPSparkJobTask(
        name = "name",
        args = List("args1", "args2"),
        mainClass = "mainClass",
        libs = List("spark.jar", "config.jar"),
        conf = Map("key1" -> "value1", "key2" -> "value2"),
        cluster = "cluster",
        project = "project",
        region = "region"
      ).toJson

      val jsonString = """"""
      assertTrue(json == jsonString)
    }
}
