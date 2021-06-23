package etlflow.etlsteps

import etlflow.coretests.Schema.EtlJob3Props
import etlflow.coretests.TestSuiteHelper
import etlflow.db.RunDbMigration
import etlflow.log.ApplicationLogger
import etlflow.schema.Credential.JDBC
import etlflow.utils.Configuration
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object JobsTestSuite extends DefaultRunnableSpec with Configuration with ApplicationLogger with TestSuiteHelper {

  val credentials: JDBC = config.dbLog
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job Http") {
        val job = HttpSmtpSteps(EtlJob3Props())
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ).provideCustomLayerShared(testDBLayer.orDie)
}
