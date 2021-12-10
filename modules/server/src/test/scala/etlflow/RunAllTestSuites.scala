package etlflow

import etlflow.db.RunDbMigration
import etlflow.etlsteps._
import etlflow.executor._
import etlflow.jobtests.ConfigHelper
import etlflow.jobtests.jobs.JobsTestSuite
import etlflow.utils._
import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with ConfigHelper {

  zio.Runtime.default.unsafeRun(RunDbMigration(config.db.get,clean = true))

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Job Test Suites") (
    JobsTestSuite(config).spec,
    CredentialStepTestSuite(config).spec,
    DBStepTestSuite(config).spec,
    EtlFlowJobStepTestSuite(config).spec,
    LocalExecutorTestSuite.spec,
    LocalSubProcessExecutorTestSuite.spec,
    //SlackLoggingTestSuite.spec,
    ReflectionTestSuite.spec,
  ).provideCustomLayerShared(etlflow.log.nolog ++ etlflow.json.Implementation.live ++ etlflow.crypto.Implementation.live(None))
}
