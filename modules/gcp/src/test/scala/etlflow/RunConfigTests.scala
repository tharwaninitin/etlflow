package etlflow

import etlflow.config._
import zio.test._

object RunConfigTests extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] = suite("GCP Config Checks")(
    DPSparkJobTaskConfig.spec
  ) @@ TestAspect.sequential
}
