package etlflow.coretests.steps.master

import etlflow.coretests.Schema.EtlJob1Props
import etlflow.etlsteps.EtlFlowJobStep
import etlflow.coretests.jobs.Job1HelloWorld
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object EtlFlowJobStepTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute EtlFlowJobStep") {

        val step = EtlFlowJobStep[EtlJob1Props](
          name = "Test",
          job  = Job1HelloWorld(EtlJob1Props()),
        )

        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
  )
}
