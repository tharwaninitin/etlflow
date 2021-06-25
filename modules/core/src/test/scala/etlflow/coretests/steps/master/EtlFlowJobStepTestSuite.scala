package etlflow.coretests.steps.master

import etlflow.coretests.Schema.EtlJob1Props
import etlflow.coretests.TestSuiteHelper
import etlflow.coretests.jobs.Job1HelloWorld
import etlflow.etlsteps.EtlFlowJobStep
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object EtlFlowJobStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper  {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlowJobStepTestSuite") (
      testM("Execute EtlFlowJobStep") {
        val step = EtlFlowJobStep[EtlJob1Props](
          name = "Test",
          job  = Job1HelloWorld(EtlJob1Props()),
        ).process().provideCustomLayer(fullLayer)
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
  )
}
