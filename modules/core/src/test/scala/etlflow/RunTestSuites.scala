package etlflow

import etlflow.log.ApplicationLogger
import zio.test._
import zio.{Runtime, ULayer}

object RunTestSuites extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  def spec: Spec[TestEnvironment, Any] = (suite("Core Test Suites")(
//    DateTimeAPITestSuite.spec,
//    RetryTaskTestSuite.spec,
//    GenericTaskTestSuite.spec,
//    ErrorHandlingTestSuite.spec,
    PipelineTestSuite.spec
  ) @@ TestAspect.sequential).provideShared(audit.noop ++ Runtime.removeDefaultLoggers)
}
