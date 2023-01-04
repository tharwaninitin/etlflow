package etlflow

import etlflow.json._
import etlflow.log.ApplicationLogger
import zio.ULayer
import zio.test._

object RunJsonTests extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  override def spec: Spec[TestEnvironment, Any] = suite("GCP Config Checks")(
    DPSparkJobTaskJson.spec
  ) @@ TestAspect.sequential
}
