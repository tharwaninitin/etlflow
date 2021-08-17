package etlflow

import etlflow.api.ApiTestSuite
import etlflow.db.RunDbMigration
import etlflow.scheduler.{ParseCronTestSuite, SchedulerTestSuite}
import etlflow.utils.{CorsConfigTestSuite, GetCronJobTestSuite, RequestValidatorTestSuite, SetTimeZoneTestSuite}
import etlflow.webserver.{AuthenticationTestSuite, WebSocketApiTestSuite, NewRestTestSuite, OldRestTestSuite, WebSocketHttpTestSuite}
import zio.test._
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.ZLayer

object RunAllTestSuites extends DefaultRunnableSpec with ServerSuiteHelper {
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))
  
  val httpenv = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto
  
  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Server Test Suites") (
    SchedulerTestSuite.spec,
    ApiTestSuite(config.db).spec,
    CorsConfigTestSuite.spec,
    GetCronJobTestSuite.spec,
    RequestValidatorTestSuite.spec,
    SetTimeZoneTestSuite(config).spec,
    WebSocketApiTestSuite(auth).spec,
    ParseCronTestSuite.spec,
    AuthenticationTestSuite(config.db, 8081).spec,
    NewRestTestSuite(8080).spec,
    OldRestTestSuite(8088).spec,
    WebSocketHttpTestSuite(8083).spec,
  ).provideCustomLayerShared(httpenv ++ fullLayer.orDie)
}
