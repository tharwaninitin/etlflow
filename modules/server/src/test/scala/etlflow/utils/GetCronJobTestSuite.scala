package etlflow.utils

import cron4s.Cron
import etlflow.ServerSuiteHelper
import etlflow.api.Schema.Job
import etlflow.db.JobDBAll
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, _}

object GetCronJobTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {


  val jobDbAll = JobDBAll("Job1","","0 0 11 ? * *",0,0,true,None)
  val getCronJob1 = GetCronJob("0 0 11 ? * *", jobDbAll, "", Map.empty)
  val getCronJob2 = GetCronJob("", jobDbAll, "", Map.empty)

  val job1 = Job("Job1",Map.empty,Cron("0 0 11 ? * *").toOption, "2021-07-28T11:00", "2 hours from now (2.36 hrs)", 0,0,true,0, "")
  val job2 = Job("Job1",Map.empty,Cron("0 0 11 ? * *").toOption, "", "2 hours from now (2.36 hrs)", 0,0,true,0, "")

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("GetCronJob Test Suite")(
      test("GetCronJob Should return correct Job name time when schedule is provided") {
        assert(getCronJob1.name)(equalTo(job1.name))
      },
      test("GetCronJob Should return correct Job when schedule is not provided") {
        assert(getCronJob2.nextSchedule)(equalTo(job2.nextSchedule))
      }
    )
}
