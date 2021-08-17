package etlflow

import etlflow.webserver.NewRestTestSuite
import zio.test._

object RunNewRestTestSuites extends DefaultRunnableSpec with ServerSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("NewRest Test Suites") (
    NewRestTestSuite.spec,
  ).provideCustomLayerShared(fullLayer.orDie)
}
