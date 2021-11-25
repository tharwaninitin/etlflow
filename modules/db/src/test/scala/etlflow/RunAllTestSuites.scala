package etlflow

import etlflow.db._
import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with DbSuiteHelper {
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))
  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Db Test Suites") (
    DbLayerTestSuite.spec,
    SqlTestSuite.spec,
    UtilsTestSuite.spec
  ).provideCustomLayerShared(liveFullDB(credentials).orDie)
}
