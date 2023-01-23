package etlflow

import etlflow.audit.Audit
import etlflow.k8s.K8S
import etlflow.task._
import zio.TaskLayer
import zio.test._

object RunTests extends ZIOSpecDefault {

  private val env: TaskLayer[K8S with Audit] = K8S.live() ++ audit.noop

  override def spec: Spec[TestEnvironment, Any] = (suite("Kube Tasks")(
    K8SJobTestSuite.configSpec,
    K8SDeleteJobTestSuite.configSpec,
    K8SJobTestSuite.spec,
    K8SGetJobTestSuite.spec,
    K8SDeleteJobTestSuite.spec,
    K8SGetJobTestSuite.failing
  ) @@ TestAspect.sequential).provide(env.orDie)
}
