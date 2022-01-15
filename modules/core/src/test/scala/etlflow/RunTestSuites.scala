package etlflow

import etlflow.etlsteps._
import etlflow.utils._
import zio.test._

object RunTestSuites extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Utils Test Suites")(
    DateTimeAPITestSuite.spec,
    SensorStepTestSuite.spec,
    ErrorHandlingTestSuite.spec
  ) @@ TestAspect.sequential
}
