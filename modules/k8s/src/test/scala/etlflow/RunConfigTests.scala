package etlflow

import etlflow.config._
import etlflow.log.ApplicationLogger
import zio.ULayer
import zio.test._

object RunConfigTests extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  override def spec: Spec[TestEnvironment, Any] = suite("Kube Config Tasks")(
    K8SCreateTestSuite.configSpec,
    K8SDeleteTestSuite.configSpec
  ) @@ TestAspect.sequential
}
