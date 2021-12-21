package etlflow.api

import etlflow.api.Schema.{CredentialsArgs, Creds, CurrentTime, Props}
import etlflow.db._
import etlflow.schema.Credential.JDBC
import etlflow.utils.DateTimeApi.getCurrentTimestampAsString
import zio.test.Assertion.equalTo
import zio.test._

case class ApiTestSuite(credential: JDBC) {

  val jobLogs = List(
    JobLogs("EtlJobDownload","1","0"),
    JobLogs("EtlJobSpr","1","0"),
    JobLogs("Job1","2","0"),
    JobLogs("etlflow.jobtests.jobs.Job1HelloWorld","2","0")
  ).sortBy(_.job_name)
  val getCredential = List(
    GetCredential("AWS", "JDBC", "2021-07-21 12:37:19.298812"),
    GetCredential("etlflow", "jdbc", "2021-12-11 12:04:06.225525")
  )

  val spec: ZSpec[environment.TestEnvironment with ServerEnv, Any] =
    suite("Server Api")(
      testM("getInfo Test")(
        assertM(Service.getInfo.map(x => x.cron_jobs))(equalTo(0))
      ),
      testM("getCurrentTime Test")(
        assertM(Service.getCurrentTime.map(x => x))(equalTo(CurrentTime(current_time = getCurrentTimestampAsString())))
      ),
      testM("getQueueStats Test")(
        assertM(Service.getQueueStats.map(x => x))(equalTo(List.empty))
      ),
      testM("getJobLogs Test")(
        assertM(Service.getJobLogs(JobLogsArgs(None,Some(10L))).map(x => x.sortBy(_.job_name)))(equalTo(jobLogs))
      ),
      testM("getCredentials Test")(
        assertM(Service.getCredentials.map(x => x.map(_.name)))(equalTo(getCredential.map(_.name)))
      ),
      testM("getJobStats Test")(
        assertM(Service.getJobStats.map(x => x))(equalTo(List.empty))
      ),
      testM("getCacheStats Test")(
        assertM(Service.getCacheStats.map(x => x.map(y => y.name)))(equalTo(List("Login")))
      ),
      testM("addCredential Test")(
        assertM(Service.addCredentials(CredentialsArgs("AWS1",Creds.AWS,List(Props("access_key","1231242"),Props("secret_key","1231242")))).map(x => x))(equalTo(Credential("AWS1","aws","""{"access_key":"1231242","secret_key":"1231242"}""")))
      ),
      testM("updateCredential Test")(
        assertM(Service.updateCredentials(CredentialsArgs("AWS1",Creds.AWS,List(Props("access_key","1231243"),Props("secret_key","1231242")))).map(x => x))(equalTo(Credential("AWS1","aws","""{"access_key":"1231243","secret_key":"1231242"}""")))
      ),
    )@@ TestAspect.sequential
}
