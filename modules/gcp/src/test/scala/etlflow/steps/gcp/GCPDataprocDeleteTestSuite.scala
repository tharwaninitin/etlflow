package etlflow.steps.gcp

import etlflow.etlsteps.DPDeleteStep
import etlflow.schema.Executor.DATAPROC
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object GCPDataprocDeleteTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow DPDeleteStep Steps") (
      testM("Execute DPDeleteStep") {

        val dpConfig = DATAPROC(
          sys.env("DP_PROJECT_ID"),
          sys.env("DP_REGION"),
          sys.env("DP_ENDPOINT"),
          sys.env("DP_CLUSTER_NAME")
        )

        val step = DPDeleteStep(
          name     = "DPDeleteStepExample",
          config   = dpConfig,
        )
        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )

}
