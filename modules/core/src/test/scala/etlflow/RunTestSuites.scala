package etlflow

import etlflow.utils._
import zio.Runtime
import zio.test._

object RunTestSuites extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment, Any] = (suite("Core Test Suites")(
    DateTimeAPITestSuite.spec,
    RetryTaskTestSuite.spec,
    GenericTaskTestSuite.spec,
    ErrorHandlingTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomShared(log.noLog ++ Runtime.removeDefaultLoggers)
}
