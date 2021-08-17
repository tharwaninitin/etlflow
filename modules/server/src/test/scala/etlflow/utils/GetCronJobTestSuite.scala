package etlflow.utils

import etlflow.api.Schema.Job
import etlflow.db.JobDBAll
import zio.test.Assertion.equalTo
import zio.test._

object GetCronJobTestSuite {

  val jobDbAll = JobDBAll("Job1","","0 0 11 ? * *",0,0,true,None)
  val getCronJob1 = GetCronJob("0 0 11 ? * *", jobDbAll, "", Map.empty)
  val getCronJob2 = GetCronJob("", jobDbAll, "", Map.empty)

  val job1 = Job("Job1",Map.empty,"0 0 11 ? * *", "2021-07-28T11:00", "2 hours from now (2.36 hrs)", 0,0,true,0, "")
  val job2 = Job("Job1",Map.empty,"0 0 11 ? * *", "", "2 hours from now (2.36 hrs)", 0,0,true,0, "")

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("GetCronJob Test Suite")(
      test("GetCronJob Should return correct Job name time when schedule is provided") {
        assert(getCronJob1.name)(equalTo(job1.name))
      },
      test("GetCronJob Should return correct Job when schedule is not provided") {
        assert(getCronJob2.nextSchedule)(equalTo(job2.nextSchedule))
      }
    )
}
