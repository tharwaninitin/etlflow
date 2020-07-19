package etlflow.steps

import etlflow.EtlJobProps
import etlflow.etlsteps.EtlFlowJobStep
import etlflow.jobs.HelloWorldJob
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object EtlFlowJobStepTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute EtlFlowJobStep") {
        // val props = new EtlJobProps{
        //  override val job_enable_db_logging: Boolean = false
        // }
        case object HelloWorldJobProps extends EtlJobProps {
          override val job_enable_db_logging: Boolean = false
        }
        val step = EtlFlowJobStep(
          name    = "Test",
          job     = HelloWorldJob,
          props   = HelloWorldJobProps,
          conf    = None
        )
        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
  )
}
