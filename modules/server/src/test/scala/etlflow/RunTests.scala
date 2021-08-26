package etlflow

import etlflow.api.ApiTestSuite
import etlflow.db.RunDbMigration
import etlflow.executor.ExecutorTestSuite
import etlflow.scheduler.{ParseCronTestSuite, SchedulerTestSuite}
import etlflow.utils.{CorsConfigTestSuite, GetCronJobTestSuite, SetTimeZoneTestSuite}
import etlflow.webserver._
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.test._

object RunTests extends DefaultRunnableSpec with ServerSuiteHelper {
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))
  
  val httpenv = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto
  
  def spec: ZSpec[environment.TestEnvironment, Any] = (suite("Server Test Suites") (
    SchedulerTestSuite.spec,
    ApiTestSuite(config.db).spec,
    CorsConfigTestSuite.spec,
    GetCronJobTestSuite.spec,
    SetTimeZoneTestSuite(config).spec,
    WebSocketApiTestSuite(auth).spec,
    ParseCronTestSuite.spec,
    AuthenticationTestSuite(config.db, 8080).spec,
    RestTestSuite(8081).spec,
    WebSocketHttpTestSuite(8083).spec,
    ExecutorTestSuite.spec,
    GraphqlTestSuite.spec,
  )@@ TestAspect.sequential).provideCustomLayerShared(httpenv ++ fullLayer.orDie)
}
