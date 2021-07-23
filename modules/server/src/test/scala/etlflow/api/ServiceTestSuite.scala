package etlflow.api

import com.google.common.cache.CacheStats
import etlflow.api.Schema.{CacheDetails, CurrentTime, EtlJobStatus, QueueDetails}
import etlflow.db.{GetCredential, JobLogs, JobLogsArgs}
import etlflow.executor.ExecutorTestSuite.{testAPILayer, testDBLayer, testJsonLayer}
import etlflow.utils.DateTimeApi.getCurrentTimestampAsString
import zio.test.Assertion.equalTo
import zio.test._

object ServiceTestSuite extends DefaultRunnableSpec  {

  val cacheDetailsList = List(CacheDetails("login",Map("x1" -> "x2")))
  val jobLogs = List(JobLogs("EtlJobSpr","1","0"), JobLogs("EtlJobDownload","1","0"))
  val getCredential = List(GetCredential("AWS", "JDBC", "2021-07-21 12:37:19.298812"))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("DBApi Suite")(
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
        assertM(Service.getJobLogs(JobLogsArgs(None,Some(10L))).map(x => x))(equalTo(jobLogs))
      ),
      testM("getCredentials Test")(
        assertM(Service.getCredentials.map(x => x))(equalTo(getCredential))
      ),
      testM("getJobStats Test")(
        assertM(Service.getJobStats.map(x => x))(equalTo(List.empty))
      )
    )@@ TestAspect.sequential).provideCustomLayer((testAPILayer ++ testJsonLayer ++ testDBLayer).orDie)
}
