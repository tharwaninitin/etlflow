package etlflow

import etlflow.config._
import etlflow.log.ApplicationLogger
import zio.ULayer
import zio.test._

object RunConfigTests extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  override def spec: Spec[TestEnvironment, Any] = suite("BQ Config")(
    GCSTestSuite.parseSpec,
    DPTestSuite.parseSpec,
    BQTestSuite.parseSpec
  ) @@ TestAspect.sequential
}
