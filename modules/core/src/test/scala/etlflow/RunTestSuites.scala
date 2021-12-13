package etlflow

import etlflow.utils._
import zio.test._

object RunTestSuites extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Utils Test Suites")(
    DateTimeAPITestSuite.spec,
  )
}
