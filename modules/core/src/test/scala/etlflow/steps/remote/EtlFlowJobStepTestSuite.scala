package etlflow.steps.remote

import etlflow.Schema.EtlJob4Props
import etlflow.etlsteps.EtlFlowJobStep
import etlflow.jobs.HelloWorldJob
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object EtlFlowJobStepTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute EtlFlowJobStep") {

        val step = EtlFlowJobStep[EtlJob4Props](
          name = "Test",
          job  = HelloWorldJob(EtlJob4Props()),
        )

        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
  )
}
