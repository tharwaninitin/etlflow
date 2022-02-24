package etlflow.etlsteps

import etlflow.jobtests.MyEtlJobProps.EtlJob1Props
import etlflow.jobtests.jobs.Job1HelloWorld
import etlflow.log.LogEnv
import etlflow.model.Config
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

case class EtlFlowJobStepTestSuite(config: Config) {

  val step = EtlFlowJobStep[EtlJob1Props](
    name = "Test",
    job = Job1HelloWorld(EtlJob1Props())
  )

  val spec: ZSpec[environment.TestEnvironment with LogEnv, Any] =
    suite("EtlFlowJob Step")(
      testM("Execute EtlFlowJobStep") {
        assertM(step.execute.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        step.job_run_id = "123"
        assertTrue(step.getStepProperties == Map("step_run_id" -> "123"))
      }
    )
}
