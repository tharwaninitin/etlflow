package etlflow.coretests.steps

import etlflow.coretests.Schema.EtlJob1Props
import etlflow.coretests.TestSuiteHelper
import etlflow.coretests.jobs.Job1HelloWorld
import etlflow.etlsteps.{EtlFlowJobStep, SensorStep}
import zio.ZIO
import zio.test.Assertion._
import zio.test._

import scala.concurrent.duration._

object EtlFlowJobStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper with SensorStep {

  val step = EtlFlowJobStep[EtlJob1Props](
    name = "Test",
    job = Job1HelloWorld(EtlJob1Props()),
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlowJobStepTestSuite")(
      testM("Execute EtlFlowJobStep") {
        val step = EtlFlowJobStep[EtlJob1Props](
          name = "Test",
          job = Job1HelloWorld(EtlJob1Props()),
        ).process().retry(noThrowable && schedule(10, 5.second)).provideCustomLayer(fullLayer)
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        step.job_run_id = "123"
        val props = step.getStepProperties()
        assert(props)(equalTo(Map("step_run_id" -> "123")))
      }
    )
}
