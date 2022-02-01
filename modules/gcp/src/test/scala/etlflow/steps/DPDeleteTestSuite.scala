package etlflow.steps

import etlflow.etlsteps.DPDeleteStep
import etlflow.gcp.DP
import etlflow.model.Executor.DATAPROC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DPDeleteTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow DPDeleteStep Step")(
      testM("Execute DPDeleteStep") {

        val dpConfig = DATAPROC(
          sys.env("DP_PROJECT_ID"),
          sys.env("DP_REGION"),
          sys.env("DP_ENDPOINT"),
          sys.env("DP_CLUSTER_NAME")
        )

        val step = DPDeleteStep(
          name = "DPDeleteStepExample",
          config = dpConfig
        ).process.provideLayer(DP.live(dpConfig.endpoint).orDie)

        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )

}
