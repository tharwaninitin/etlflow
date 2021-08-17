package etlflow

import etlflow.webserver.WebSocketHttpTestSuite
import zio.test._

object RunWsHttpTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {
  def spec: ZSpec[environment.TestEnvironment, Any] = suite("WebSocketHttp Test Suites") (
      WebSocketHttpTestSuite.spec,
  ).provideCustomLayerShared(fullLayer.orDie)
}
