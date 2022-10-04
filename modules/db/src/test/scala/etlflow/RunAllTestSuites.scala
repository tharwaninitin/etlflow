package etlflow

import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.PlatformDefault"))
object RunAllTestSuites extends ZIOSpecDefault with DbSuiteHelper {
  private val jri   = "a27a7415-57b2-4b53-8f9b-5254e847a3011"
  private val reset = if (credentials.url.toLowerCase.contains("postgresql")) true else false
  def spec: Spec[TestEnvironment, Any] = (suite("DB Test Suites")(
    InitDBSuite.spec(reset), // Will initialize db
    db.DbTestSuite.spec,     // Will execute actual DB queries
    audit.DbTestSuite.spec,  // Will execute actual DB queries
    audit.SqlTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomShared(db.liveDBWithLog(credentials, jri).orDie)
}
