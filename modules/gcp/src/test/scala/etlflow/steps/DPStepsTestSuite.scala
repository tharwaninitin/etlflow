package etlflow.steps

import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}
import etlflow.gcp.DP
import etlflow.model.Executor.{DATAPROC, SPARK_CONF}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{assertM, environment, DefaultRunnableSpec, TestAspect, ZSpec}

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
          config = dpConfig
        ).process.provideLayer(DP.live)
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute DPSparkJob step") {
        val libs = List("file:///usr/lib/spark/examples/jars/spark-examples.jar")
        val spark_conf = List(
          SPARK_CONF("spark.executor.memory", "1g"),
          SPARK_CONF("spark.driver.memory", "1g")
        )
        val step = DPSparkJobStep(
          name = "DPSparkJobStepExample",
          args = List("1000"),
          config = dpConfig.copy(sp = spark_conf),
          main_class = "org.apache.spark.examples.SparkPi",
          libs = libs
        ).process.provideLayer(DP.live)
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
