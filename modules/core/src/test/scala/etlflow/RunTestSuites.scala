package etlflow

import etlflow.utils._
import zio.Clock.ClockLive
import zio.{Runtime, ZLayer}
import zio.test._

object RunTestSuites extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment, Any] = (suite("Utils Test Suites")(
    DateTimeAPITestSuite.spec,
    RetryTaskTestSuite.spec,
    GenericTaskTestSuite.spec,
    ErrorHandlingTestSuite.spec
  ) @@ TestAspect.sequential).provideCustomShared(log.noLog ++ ZLayer.succeed(ClockLive) ++ Runtime.removeDefaultLoggers)
}
