package etlflow.task

import etlflow.TestHelper
import etlflow.audit.AuditEnv
import gcp4zio.dp._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object DPTasksTestSuite extends TestHelper {
  val spec: Spec[TestEnvironment with DPJobEnv with AuditEnv, Any] =
    suite("EtlFlow DPJobTasks")(
      test("Execute DPHiveJob task") {
        val task = DPHiveJobTask(
          name = "DPHiveJobTaskExample",
          "SELECT 1 AS ONE",
          dpCluster,
          gcpProjectId.get,
          gcpRegion.get
        ).execute
        assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute DPSparkJob task") {
        val libs = List("file:///usr/lib/spark/examples/jars/spark-examples.jar")
        val conf = Map("spark.executor.memory" -> "1g", "spark.driver.memory" -> "1g")
        val task = DPSparkJobTask(
          name = "DPSparkJobTaskExample",
          args = List("1000"),
          mainClass = "org.apache.spark.examples.SparkPi",
          libs = libs,
          conf,
          dpCluster,
          gcpProjectId.get,
          gcpRegion.get
        ).execute
        assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
