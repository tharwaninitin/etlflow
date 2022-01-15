package etlflow.jobtests.jobs

import etlflow.etljobs.EtlJob
import etlflow.jobtests.MyEtlJobPropsMapping
import etlflow.jobtests.MyEtlJobProps.EtlJob1Props
import etlflow.model.Config
import etlflow.{CliApp, EtlJobProps}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{ZEnv, ZIO}
import zio.duration._

case class JobsTestSuite(config: Config) {

  private val app = new CliApp[MyEtlJobPropsMapping[EtlJobProps, EtlJob[EtlJobProps]]] {}
  private def job(args: List[String]): ZIO[ZEnv, Throwable, Unit] = app.cliRunner(args, config)
  private val job1                                                = Job1HelloWorld(EtlJob1Props())

  val spec = suite("EtlFlow Job")(
    testM("Execute Job1HelloWorld using EtlFlowApp") {
      val args = List("run_job", "--job_name", "Job1")
      assertM(job(args).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute Job1HelloWorld using EtlJob") {
      assertM(job1.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
  ) @@ TestAspect.sequential @@ TestAspect.eventually @@ TestAspect.timeout(1.minute)
}
