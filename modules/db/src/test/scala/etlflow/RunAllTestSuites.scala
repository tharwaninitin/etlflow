package etlflow

import etlflow.db.{DbLayerTestSuite, RunDbMigration, SqlTestSuite, UtilsTestSuite, liveDB}
import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with DbSuiteHelper {
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))
  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Db Test Suites") (
    DbLayerTestSuite.spec,
    SqlTestSuite.spec,
    UtilsTestSuite.spec
  ).provideCustomLayerShared(liveDB(credentials).orDie)
}
