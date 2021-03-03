package etlflow.steps.cloud

import etlflow.etlsteps.DPCreateStep
import etlflow.etlsteps.DPCreateStep.DPConfig
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object GCPDataprocCreateTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute DPCreateStep") {

        val dataproc_create_conf =  DPConfig(
          project_id      = sys.env("DP_PROJECT_ID"),
          region          = sys.env("DP_REGION"),
          endpoint        = sys.env("DP_ENDPOINT"),
          bucket_name     = sys.env("DP_BUCKET_NAME"),
          subnet_work_uri = sys.env("DP_SUBNET_WORK_URI"),
          all_tags        = List(sys.env("DP_ALL_TAGS")),
          service_account = Some(sys.env("DP_SERVICE_ACCOUNT"))
        )

        val step = DPCreateStep(
          name                        = "DPCreateStepExample",
          cluster_name                = sys.env("DP_CLUSTER_NAME"),
          props                       = dataproc_create_conf
        )
        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }

    )
}
