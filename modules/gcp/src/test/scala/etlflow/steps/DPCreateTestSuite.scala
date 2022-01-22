package etlflow.steps

import etlflow.etlsteps.DPCreateStep
import etlflow.gcp.DataprocProperties
import etlflow.model.Executor.DATAPROC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{assertM, environment, DefaultRunnableSpec, ZSpec}

object DPCreateTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps")(
      testM("Execute DPCreateStep") {

        val dpConfig = DATAPROC(
          sys.env("DP_PROJECT_ID"),
          sys.env("DP_REGION"),
          sys.env("DP_ENDPOINT"),
          sys.env("DP_CLUSTER_NAME")
        )

        val dpProps = DataprocProperties(
          bucket_name = sys.env("DP_BUCKET_NAME"),
          subnet_uri = sys.env.get("DP_SUBNET_WORK_URI"),
          network_tags = sys.env("DP_NETWORK_TAGS").split(",").toList,
          service_account = sys.env.get("DP_SERVICE_ACCOUNT")
        )

        val step = DPCreateStep(
          name = "DPCreateStepExample",
          config = dpConfig,
          props = dpProps
        )
        assertM(step.process.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}
