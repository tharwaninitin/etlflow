package etlflow.steps.cloud

import etlflow.etlsteps.DPCreateStep.DPConfig
import etlflow.etlsteps.DPDeleteStep
import etlflow.etlsteps.DPDeleteStep.DPDeleteConfig
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object GCPDataprocDeleteTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow DPDeleteStep Steps") (
      testM("Execute DPDeleteStep") {

        val dataproc_delete_conf =  DPDeleteConfig(
          project_id      = sys.env("DP_PROJECT_ID"),
          region          = sys.env("DP_REGION"),
          endpoint        = sys.env("DP_ENDPOINT")
        )

        val step = DPDeleteStep(
          name                    = "DPDeleteStepExample",
          cluster_name            = sys.env("DP_CLUSTER_NAME"),
          props                   = dataproc_delete_conf
        )
        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )

}
