package etlflow.jobs

import etlflow.coretests.Schema.EtlJobDeltaLake
import etlflow.coretests.TestSuiteHelper
import etlflow.coretests.steps.master.EtlFlowJobStepTestSuite.config
import etlflow.jdbc.liveDBWithTransactor
import etlflow.schema.Credential.JDBC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object JobsTestSuite extends DefaultRunnableSpec with TestSuiteHelper  {

  val credentials: JDBC = config.dbLog
  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Jobs") (
      testM("Execute Delta Lake Step") {
        val job = Job4SparkDeltaStep(EtlJobDeltaLake())
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ).provideCustomLayerShared(testDBLayer.orDie)
}
