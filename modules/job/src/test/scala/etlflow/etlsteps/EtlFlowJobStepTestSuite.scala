package etlflow.etlsteps

import etlflow.CoreEnv
import etlflow.jobtests.MyEtlJobProps.EtlJob1Props
import etlflow.jobtests.jobs.Job1HelloWorld
import etlflow.schema.Config
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

case class EtlFlowJobStepTestSuite(config: Config) {

  val step = EtlFlowJobStep[EtlJob1Props](
    name = "Test",
    job = Job1HelloWorld(EtlJob1Props()),
  )

  val spec: ZSpec[environment.TestEnvironment with CoreEnv, Any] =
    suite("EtlFlowJob Step")(
      testM("Execute EtlFlowJobStep") {
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        step.job_run_id = "123"
        assert(step.getStepProperties())(equalTo(Map("step_run_id" -> "123")))
      }
    )
}
