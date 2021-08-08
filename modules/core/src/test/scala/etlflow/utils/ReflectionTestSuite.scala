package etlflow.utils

import etlflow.utils.ReflectionHelper._
import etlflow.utils.{ReflectAPI => RF}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object ReflectionTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("Reflect Api Test Cases")(
      test("getTypeFullName[MEJP] should should return etlflow.coretests.MyEtlJobPropsMapping") {
        assert(RF.getTypeFullName[MEJP])(equalTo("etlflow.coretests.MyEtlJobPropsMapping"))
      },
//      testM("getSubClasses[EtlJobName] should should retrieve Set successfully") {
//        assertM(RF.getSubClasses[EtlJobName])(equalTo(Set("Job1", "Job2", "Job3", "Job4", "Job5")))
//      },
//      testM("getSubClasses[EtlJobTest] should  should retrieve Set successfully") {
//        assertM(RF.getSubClasses[EtlJobTest])(equalTo(Set("Job1", "Job2")))
//      },
      testM("getSubClasses[MEJP] should retrieve Set successfully") {
        assertM(RF.getSubClasses[MEJP])(equalTo(Set("Job1", "Job2", "Job3", "Job4", "Job5", "Job6", "Job7", "Job8", "Job9")))
      },
      testM("getJob should return correct error message in case of invalid job name") {
        val ejpm = RF.getJob[MEJP]("JobM")
        assertM(ejpm.foldM(ex => ZIO.succeed(ex.getMessage), op => ZIO.succeed(op)))(equalTo("JobM not present"))
      },
      testM("getJob[MEJP](Job1) should return Job1") {
        val ejpm = RF
                    .getJob[MEJP]("Job1")
                    .map(jpm => (jpm.toString,jpm.job_name,jpm.job_props_name,jpm.etlJob(Map.empty).toString))
        assertM(ejpm)(equalTo(("Job1","etlflow.coretests.jobs.Job1HelloWorld","etlflow.coretests.Schema.EtlJob1Props","Job1HelloWorld(EtlJob1Props())")))
      },
      testM("getJobs[MEJP] should return list of jobs") {
        val ejpm = RF.getJobs[MEJP].map(_.map(_.name).sorted)
        assertM(ejpm)(equalTo(List("Job1", "Job2", "Job3", "Job4", "Job5", "Job6", "Job7", "Job8", "Job9")))
      }
    ) @@ TestAspect.sequential
  }
}
