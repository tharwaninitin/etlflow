package etlflow.steps

import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}
import etlflow.schema.Executor.DATAPROC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, TestAspect, ZSpec, assertM, environment}

object DPStepsTestSuite extends DefaultRunnableSpec {

  val dpConfig: DATAPROC = DATAPROC(
    sys.env("DP_PROJECT_ID"),
    sys.env("DP_REGION"),
    sys.env("DP_ENDPOINT"),
    sys.env("DP_CLUSTER_NAME")
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps")(
      testM("Execute DPHiveJob step") {
        val step = DPHiveJobStep(
          name = "DPHiveJobStepExample",
          query = "SELECT 1 AS ONE",
          config = dpConfig,
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute DPSparkJob step") {
        val libs = List("file:///usr/lib/spark/examples/jars/spark-examples.jar")
        val step = DPSparkJobStep(
          name = "DPSparkJobStepExample",
          args = List("1000"),
          config = dpConfig,
          main_class = "org.apache.spark.examples.SparkPi",
          libs = libs
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
