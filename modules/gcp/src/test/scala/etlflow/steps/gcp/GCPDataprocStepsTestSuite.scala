package etlflow.steps.gcp

import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}
import etlflow.schema.Executor.DATAPROC
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object GCPDataprocStepsTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute DPHiveJob step") {
        val dpConfig = DATAPROC(
          sys.env("DP_PROJECT_ID"),
          sys.env("DP_REGION"),
          sys.env("DP_ENDPOINT"),
          sys.env("DP_CLUSTER_NAME")
        )
        val step = DPHiveJobStep(
          name = "DPHiveJobStepExample",
          query = "SELECT 1 AS ONE",
          config = dpConfig,
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute DPSparkJob step") {
        val dpConfig = DATAPROC(
          sys.env("DP_PROJECT_ID"),
          sys.env("DP_REGION"),
          sys.env("DP_ENDPOINT"),
          sys.env("DP_CLUSTER_NAME")
        )
        val libs = sys.env("DP_LIBS").split(",").toList
        val step = DPSparkJobStep(
          name        = "DPSparkJobStepExample",
          job_name    = sys.env("DP_JOB_NAME"),
          props       = Map.empty,
          config      = dpConfig,
          main_class  = sys.env("DP_MAIN_CLASS"),
          libs        = libs
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}
