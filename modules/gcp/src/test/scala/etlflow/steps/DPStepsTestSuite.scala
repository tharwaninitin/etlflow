package etlflow.steps

import etlflow.TestHelper
import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}
import etlflow.log.LogEnv
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object DPStepsTestSuite extends TestHelper {
  val spec: ZSpec[environment.TestEnvironment with DPJobEnv with LogEnv, Any] =
    suite("EtlFlow DPJobSteps")(
      testM("Execute DPHiveJob step") {
        val step = DPHiveJobStep(
          name = "DPHiveJobStepExample",
          "SELECT 1 AS ONE",
          dpCluster,
          gcpProjectId.get,
          gcpRegion.get
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute DPSparkJob step") {
        val libs = List("file:///usr/lib/spark/examples/jars/spark-examples.jar")
        val conf = Map("spark.executor.memory" -> "1g", "spark.driver.memory" -> "1g")
        val step = DPSparkJobStep(
          name = "DPSparkJobStepExample",
          args = List("1000"),
          mainClass = "org.apache.spark.examples.SparkPi",
          libs = libs,
          conf,
          dpCluster,
          gcpProjectId.get,
          gcpRegion.get
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
