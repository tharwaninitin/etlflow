package etlflow.steps.cloud

import etlflow.etlsteps.{DPCreateStep, DPDeleteStep, DPHiveJobStep, DPSparkJobStep}
import etlflow.utils.Executor.DATAPROC
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object GCPDataprocDeleteTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow DPDeleteStep Steps") (
      testM("Execute DPDeleteStep") {
        val delete_cluster_spd_props: Map[String, String] =
          Map(
            "project_id" -> sys.env("DP_PROJECT_ID"),
            "region" -> sys.env("DP_REGION"),
            "endpoint"-> sys.env("DP_ENDPOINT"),
          )
        val step = DPDeleteStep(
          name                    = "DPDeleteStepExample",
          cluster_name            = "test",
          props                   = delete_cluster_spd_props
        )
        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )

}
