package etlflow.config

import etlflow.task.DPSparkJobTask
import zio.test.Assertion.equalTo
import zio.test._
import zio.{ConfigProvider, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object DPSparkJobTaskConfig {
  val spec: Spec[Any, Any] =
    test("Execute DPSparkJobTaskConfig") {
      val map = Map(
        "name" -> "test",
        "args" -> "1,2,3",
        "libs" -> "1,2,3",
        // "conf"      -> "key1=value1,key2=value2",
        "mainClass" -> "test",
        "cluster"   -> "test",
        "project"   -> "test",
        "region"    -> "test"
      )
      val config = ConfigProvider.fromMap(map).load(DPSparkJobTask.config)
      assertZIO(config.foldZIO(ex => ZIO.fail(ex.getMessage), op => ZIO.logInfo(op.toString) *> ZIO.succeed("ok")))(equalTo("ok"))
    }
}
