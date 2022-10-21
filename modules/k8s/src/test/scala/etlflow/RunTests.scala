package etlflow

import etlflow.task.CreateK8sJobTestSuite
import zio.test._

object RunTests extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = suite("K8s Tasks")(
    CreateK8sJobTestSuite.spec
//    GetK8sJobTestSuite.spec,
//    GetK8sPodLogTestSuite.spec @@ TestAspect.ignore
  ) @@ TestAspect.sequential
}
