package etlflow

import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with DbSuiteHelper {
  val jri = "a27a7415-57b2-4b53-8f9b-5254e847a3011"
  def spec: ZSpec[environment.TestEnvironment, Any] = (suite("DB Test Suites")(
    InitDBSuite.spec,     // Will initialize db
    db.DbTestSuite.spec,  // Will execute actual DB queries
    log.DbTestSuite.spec, // Will execute actual DB queries
    log.SqlTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomLayerShared(db.liveDBWithLog(credentials, jri).orDie)
}
