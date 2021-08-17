package etlflow

import etlflow.api.ApiTestSuite
import etlflow.db.RunDbMigration
import etlflow.executor.ExecutorTestSuite
import etlflow.scheduler.{ParseCronTestSuite, SchedulerTestSuite}
import etlflow.utils.{CorsConfigTestSuite, GetCronJobTestSuite, RequestValidatorTestSuite, SetTimeZoneTestSuite}
import etlflow.webserver.{AuthenticationTestSuite, GraphqlTestSuite, NewRestTestSuite, OldRestTestSuite, WebSocketApiTestSuite, WebSocketHttpTestSuite}
import zio.test._
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.ZLayer

object RunTests extends DefaultRunnableSpec with ServerSuiteHelper {
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))
  
  val httpenv = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto
  
  def spec: ZSpec[environment.TestEnvironment, Any] = (suite("Server Test Suites") (
    SchedulerTestSuite.spec,
    ApiTestSuite(config.db).spec,
    CorsConfigTestSuite.spec,
    GetCronJobTestSuite.spec,
    RequestValidatorTestSuite.spec,
    SetTimeZoneTestSuite(config).spec,
    WebSocketApiTestSuite(auth).spec,
    ParseCronTestSuite.spec,
    AuthenticationTestSuite(config.db, 8080).spec,
    NewRestTestSuite(8081).spec,
    OldRestTestSuite(8082).spec,
    WebSocketHttpTestSuite(8083).spec,
    ExecutorTestSuite.spec,
    GraphqlTestSuite.spec,
  )@@ TestAspect.sequential).provideCustomLayerShared(httpenv ++ fullLayer.orDie)
}
