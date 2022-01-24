package etlflow.utils

import etlflow.server.model.Job
import etlflow.server.model.JobDBAll
import zio.test._

object GetCronJobTestSuite {

  val jobDbAll    = JobDBAll("Job1", "", "0 0 11 ? * *", 0, 0, true, None)
  val getCronJob1 = GetJob("0 0 11 ? * *", jobDbAll, "", Map.empty)
  val getCronJob2 = GetJob("", jobDbAll, "", Map.empty)

  val job1 =
    Job("Job1", Map.empty, "0 0 11 ? * *", "2021-07-28T11:00", "2 hours from now (2.36 hrs)", 0, 0, true, 0, "")
  val job2 = Job("Job1", Map.empty, "0 0 11 ? * *", "", "2 hours from now (2.36 hrs)", 0, 0, true, 0, "")

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("GetCronJob")(
      test("GetCronJob should return correct Job name time when schedule is provided") {
        assertTrue(getCronJob1.name == job1.name)
      },
      test("GetCronJob should return correct Job when schedule is not provided") {
        assertTrue(getCronJob2.nextSchedule == job2.nextSchedule)
      }
    )
}
