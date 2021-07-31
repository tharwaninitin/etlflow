package etlflow.coretests.jobs

import etlflow.coretests.{MyEtlJobPropsMapping, TestSuiteHelper}
import etlflow.coretests.Schema.EtlJob1Props
import etlflow.coretests.steps.db.DBStepTestSuite.MEJP
import etlflow.etljobs.EtlJob
import etlflow.{EtlFlowApp, EtlJobProps}
import zio.{ZEnv, ZIO}
import zio.test.Assertion.equalTo
import zio.test._
import etlflow.utils.{ReflectAPI => RF}

object JobsTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  private val app = new EtlFlowApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] {}
  private def job(args: List[String]): ZIO[ZEnv, Throwable, Unit] = app.cliRunner(args,config)

  val job1 = Job1HelloWorld(EtlJob1Props())

  def spec: ZSpec[environment.TestEnvironment, Any] =
  suite("EtlFlow Jobs") (
      testM("Execute Job1HelloWorld") {
        val args = List("run_job", "--job_name", "Job1")
        assertM(job(args).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getJobInfo") {
        val jobInfo = job1.getJobInfo()
        assert(jobInfo.map(x=>x._1))(equalTo(List("ProcessData")))
      },
      test("Execute printJobInfo") {
        job1.printJobInfo()
        assert("Ok")(equalTo("Ok"))
      },
      test("Execute printEtlJobs") {
        RF.printEtlJobs[MEJP]()
        assert("Ok")(equalTo("Ok"))
      }
    ) @@ TestAspect.sequential
}
