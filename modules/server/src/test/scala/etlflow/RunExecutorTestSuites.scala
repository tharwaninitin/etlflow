package etlflow

import etlflow.db.RunDbMigration
import etlflow.executor.ExecutorTestSuite
import zio.test._

object RunExecutorTestSuites extends DefaultRunnableSpec with ServerSuiteHelper {

  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Executor Test Suites") (
      ExecutorTestSuite.spec,
  ).provideCustomLayerShared(fullLayer.orDie)
}
