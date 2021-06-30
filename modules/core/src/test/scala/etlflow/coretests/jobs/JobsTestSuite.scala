package etlflow.coretests.jobs

import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob
import etlflow.{EtlFlowApp, EtlJobProps}
import zio.{ZEnv, ZIO}
import zio.test.Assertion.equalTo
import zio.test._

object JobsTestSuite extends DefaultRunnableSpec {

  private val app = new EtlFlowApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] {}
  private def job(args: List[String]): ZIO[ZEnv, Throwable, Unit] = app.cliRunner(args)

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Jobs") (
      testM("Execute Job1HelloWorld") {
        val args = List("run_job", "--job_name", "Job5")
        assertM(job(args).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
