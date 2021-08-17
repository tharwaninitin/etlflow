package etlflow

import etlflow.api.ApiTestSuite
import etlflow.db.RunDbMigration
import etlflow.scheduler.{ParseCronTestSuite, SchedulerPackageTestSuite, SchedulerTestSuite}
import etlflow.utils.{CorsConfigTestSuite, GetCronJobTestSuite, RequestValidatorTestSuite, SetTimeZoneTestSuite}
import etlflow.webserver.{AuthenticationTestSuite, WebSocketApiTestSuite}
import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with ServerSuiteHelper {
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Server Test Suites") (
    SchedulerTestSuite.spec,
    ApiTestSuite(config.db).spec,
    SchedulerPackageTestSuite.spec,
    CorsConfigTestSuite.spec,
    GetCronJobTestSuite.spec,
    RequestValidatorTestSuite.spec,
    SetTimeZoneTestSuite(config).spec,
    AuthenticationTestSuite(config.db).spec,
    WebSocketApiTestSuite(auth).spec,
    ParseCronTestSuite.spec
  ).provideCustomLayerShared(fullLayer.orDie)
}
