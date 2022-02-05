package etlflow

import etlflow.etlsteps.{CredentialStepTestSuite, DBStepTestSuite, EtlFlowJobStepTestSuite}
import etlflow.executor.{LocalExecutorTestSuite, LocalSubProcessExecutorTestSuite, ServerExecutorTestSuite}
import etlflow.jobtests.jobs.JobsTestSuite
import etlflow.json.JsonTestSuite
import etlflow.scheduler.SchedulerTestSuite
import etlflow.server.ServerApiTestSuite
import etlflow.utils.{CorsConfigTestSuite, GetCronJobTestSuite, ReflectionTestSuite, SetTimeZoneTestSuite}
import etlflow.webserver._
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.test._

object RunTests extends DefaultRunnableSpec with ServerSuiteHelper {

  zio.Runtime.default.unsafeRun(ResetServerDB.live.provideLayer(db.liveDB(credentials)))

  val httpenv = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto

  def spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("Server Test Suites")(
      JsonTestSuite.spec,
      JobsTestSuite(config).spec,
      CredentialStepTestSuite(config).spec,
      DBStepTestSuite(config).spec,
      EtlFlowJobStepTestSuite(config).spec,
      LocalExecutorTestSuite(config).spec,
      LocalSubProcessExecutorTestSuite.spec,
      // SlackLoggingTestSuite.spec,
      ReflectionTestSuite.spec,
      SchedulerTestSuite.spec,
      ServerApiTestSuite(config.db.get).spec,
      CorsConfigTestSuite.spec,
      GetCronJobTestSuite.spec,
      SetTimeZoneTestSuite(config).spec,
      WebSocketApiTestSuite(auth).spec,
      AuthenticationTestSuite(config.db.get, 8080).spec,
      RestTestSuite(8081).spec,
      // WebSocketHttpTestSuite(8083).spec,
      ServerExecutorTestSuite.spec,
      GraphqlTestSuite.spec
    ) @@ TestAspect.sequential
  }.provideCustomLayerShared(httpenv ++ fullLayer.orDie)
}
