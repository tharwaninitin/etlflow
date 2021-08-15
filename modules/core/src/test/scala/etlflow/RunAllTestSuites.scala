package etlflow

import etlflow.coretests.TestSuiteHelper
import etlflow.coretests.jobs.JobsTestSuite
import etlflow.db.RunDbMigration
import etlflow.etlsteps._
import etlflow.executor._
import etlflow.log._
import etlflow.utils._
import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with TestSuiteHelper {

  zio.Runtime.default.unsafeRun(RunDbMigration(config.db,clean = true))

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Core Test Suites") (
    JobsTestSuite(config).spec,
    CredentialStepTestSuite(config).spec,
    DBStepTestSuite(config).spec,
    EtlFlowJobStepTestSuite(config).spec,
    ParallelStepTestSuite(config).spec,
    SensorStepTestSuite.spec,
    LocalExecutorTestSuite.spec,
    LocalSubProcessExecutorTestSuite.spec,
    SlackLoggingTestSuite.spec,
    DateTimeAPITestSuite.spec,
    ReflectionTestSuite.spec,
  ).provideCustomLayerShared(fullLayer.orDie)
}
