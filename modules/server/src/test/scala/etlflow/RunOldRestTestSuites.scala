package etlflow

import etlflow.webserver.OldRestTestSuite
import zio.test._

object RunOldRestTestSuites extends DefaultRunnableSpec with ServerSuiteHelper {
  def spec: ZSpec[environment.TestEnvironment, Any] = suite("OldRest Test Suites") (
      OldRestTestSuite.spec,
  ).provideCustomLayerShared(fullLayer.orDie)
}
