package etlflow.server

import etlflow.model.Credential.JDBC
import etlflow.server.model._
import etlflow.utils.DateTimeApi.getCurrentTimestampAsString
import zio.test.Assertion.equalTo
import zio.test.{assertM, environment, suite, testM, TestAspect, ZSpec}

case class ServerApiTestSuite(credential: JDBC) {

  val jobLogs = List(
    JobLogs("EtlJobDownload", "1", "0"),
    JobLogs("EtlJobSpr", "1", "0"),
    JobLogs("etlflow.jobtests.jobs.Job1HelloWorld", "2", "0")
  ).sortBy(_.job_name)

  val getCredential = List(
    GetCredential("AWS", "JDBC", "2021-07-21 12:37:19.298812"),
    GetCredential("etlflow", "jdbc", "2021-12-11 12:04:06.225525")
  )

  val opCred = Credential(
    "AWS1",
    "aws",
    """{"access_key":"oCeOP7W5awGX8R9GkWRUvg==","secret_key":"oCeOP7W5awGX8R9GkWRUvg=="}"""
  )

  val spec: ZSpec[environment.TestEnvironment with ServerEnv, Any] =
    suite("Server Api")(
      testM("getInfo Test")(
        assertM(Service.getMetrics.map(x => x.cron_jobs))(equalTo(0))
      ),
      testM("getCurrentTime Test")(
        assertM(Service.getCurrentTime.map(x => x))(equalTo(getCurrentTimestampAsString()))
      ) @@ TestAspect.flaky,
      testM("getJobLogs Test")(
        assertM(Service.getJobLogs(JobLogsArgs(None, Some(10L))).map(x => x.filter(_.job_name != "Job1").sortBy(_.job_name)))(
          equalTo(jobLogs)
        )
      ),
      testM("getCredentials Test")(
        assertM(Service.getCredentials.map(x => x.map(_.name)))(equalTo(getCredential.map(_.name)))
      ),
      testM("addCredential Test")(
        assertM(
          Service
            .addCredential(
              CredentialsArgs("AWS1", Creds.AWS, List(Props("access_key", "1231242"), Props("secret_key", "1231242")))
            )
            .map(x => x)
        )(equalTo(opCred))
      ),
      testM("updateCredential Test")(
        assertM(
          Service
            .updateCredential(
              CredentialsArgs("AWS1", Creds.AWS, List(Props("access_key", "1231242"), Props("secret_key", "1231242")))
            )
            .map(x => x)
        )(equalTo(opCred))
      )
    ) @@ TestAspect.sequential
}
