package etlflow.redis

import etlflow.coretests.Schema.EtlJob3Props
import etlflow.coretests.TestSuiteHelper
import etlflow.log.ApplicationLogger
import etlflow.utils.{Configuration, DbManager}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object JobsTestSuite extends DefaultRunnableSpec with Configuration with DbManager with ApplicationLogger  with  TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job Redis") {
        val job = RedisSteps(EtlJob3Props())
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ).provideCustomLayerShared(testDBLayer.orDie)
}
