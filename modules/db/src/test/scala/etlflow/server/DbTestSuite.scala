package etlflow.server

import etlflow.db._
import zio.test.Assertion.equalTo
import zio.test.{TestAspect, ZSpec, assertM, environment, suite, testM}

object DbTestSuite {

  val jobDbAll = List(JobDBAll("Job1", "", "", 0, 0, true, None), JobDBAll("Job2", "", "", 0, 0, false, None), JobDBAll("Job3", "", "", 0, 0, true, None))
  val stepRuns = List(
    StepRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "download_spr", "{}", "pass", "", "1.6 mins", "GenericEtlStep", "123")
  )
  val jobRuns = List(
    JobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "EtlJobDownload", "{}", "pass", "", "", "GenericEtlJob", "true"),
    JobRun("a27a7415-57b2-4b53-8f9b-5254e847a302", "EtlJobSpr", "{}", "pass", "", "", "GenericEtlJob", "true")
  )

  case class TestDb(name: String)

  val jobLogs = List(JobLogs("EtlJobDownload", "1", "0"), JobLogs("EtlJobSpr", "1", "0"))
  val getCredential = List(GetCredential("AWS", "JDBC", "2021-07-21 12:37:19.298812"))

  val spec: ZSpec[environment.TestEnvironment with DBServerEnv, Any] =
    suite("DB(server) Suite")(
      testM("getUser Test")(
        assertM(DBServerApi.getUser("admin").map(x => x.user_name))(equalTo("admin"))
      ),
      testM("getJob Test")(
        assertM(DBServerApi.getJob("Job1").map(x => x))(equalTo(JobDB("Job1", "", true)))
      ),
      testM("getJobs Test")(
        assertM(DBServerApi.getJobs.map(x => x))(equalTo(jobDbAll))
      ),
      testM("getStepRuns Test")(
        assertM(DBServerApi.getStepRuns(DbStepRunArgs("a27a7415-57b2-4b53-8f9b-5254e847a301")).map(x => x.map(sr => sr.copy(start_time = "")).sortBy(_.job_run_id)))(equalTo(stepRuns.sortBy(_.job_run_id)))
      ),
      testM("getJobRuns Test")(
        assertM(DBServerApi.getJobRuns(DbJobRunArgs(None, None, None, None, None, 10L, 0L)).map(x => x.map(sr => sr.copy(start_time = "")).sortBy(_.job_name)))(equalTo(jobRuns.sortBy(_.job_name)))
      ),
      testM("getJobLogs Test")(
        assertM(DBServerApi.getJobLogs(JobLogsArgs(None, Some(10L))).map(x => x.sortBy(_.job_name)))(equalTo(jobLogs.sortBy(_.job_name)))
      ),
      testM("getCredentials Test")(
        assertM(DBServerApi.getCredentials.map(x => x))(equalTo(getCredential))
      ),
      testM("updateSuccessJob Test")(
        assertM(DBServerApi.updateSuccessJob("Job1", 1L).map(x => x))(equalTo(1L))
      ),
      testM("updateFailedJob Test")(
        assertM(DBServerApi.updateFailedJob("Job1", 1L).map(x => x))(equalTo(1L))
      ),
      testM("updateJobState Test")(
        assertM(DBServerApi.updateJobState(EtlJobStateArgs("Job1", true)).map(x => x))(equalTo(true))
      ),
      testM("addCredential Test")(
        assertM(DBServerApi.addCredential(Credential("AWS1", "JDBC", "{}")).map(x => x))(equalTo(Credential("AWS1", "JDBC", "{}")))
      ),
      testM("updateCredential Test")(
        assertM(DBServerApi.updateCredential(Credential("AWS1", "JDBC", "{}")).map(x => x))(equalTo(Credential("AWS1", "JDBC", "{}")))
      ),
      testM("refreshJobs Test")(
        assertM(DBServerApi.refreshJobs(List(EtlJob("Job1", Map.empty), EtlJob("Job2", Map.empty))).map(x => x))(equalTo(List(JobDB("Job1", "", true), JobDB("Job2", "", false))))
      )
    ) @@ TestAspect.sequential
}
