package etlflow

import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with DbSuiteHelper {
  def spec: ZSpec[environment.TestEnvironment, Any] = (suite("DB Test Suites") (
    InitDBSuite.spec,           // Will initialize db
    server.DbTestSuite.spec,    // Will execute actual DB queries
    db.DbTestSuite.spec,        // Will execute actual DB queries
    log.DbTestSuite.spec,       // Will execute actual DB queries
    server.SqlTestSuite.spec,
    log.SqlTestSuite.spec,
    db.UtilsTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomLayerShared(db.liveFullDB(credentials).orDie)
}
