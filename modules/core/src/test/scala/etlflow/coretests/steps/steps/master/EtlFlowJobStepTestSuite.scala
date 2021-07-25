package etlflow.coretests.steps.steps.master

import etlflow.coretests.Schema.EtlJob1Props
import etlflow.coretests.TestSuiteHelper
import etlflow.coretests.jobs.Job1HelloWorld
import etlflow.etlsteps.{EtlFlowJobStep, SensorStep}
import zio.ZIO
import zio.test.Assertion._
import zio.test._

import scala.concurrent.duration._


object EtlFlowJobStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper  with SensorStep {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlowJobStepTestSuite") (
      testM("Execute EtlFlowJobStep") {
        val step = EtlFlowJobStep[EtlJob1Props](
          name = "Test",
          job  = Job1HelloWorld(EtlJob1Props()),
        ).process().retry(noThrowable && schedule(10,5.second)).provideCustomLayer(fullLayer)
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
  )
}
