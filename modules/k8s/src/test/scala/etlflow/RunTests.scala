package etlflow

import etlflow.audit.Audit
import etlflow.k8s.K8S
import etlflow.task._
import zio.TaskLayer
import zio.test._

object RunTests extends ZIOSpecDefault {

  private val env: TaskLayer[K8S with Audit] = K8S.live() ++ audit.test

  override def spec: Spec[TestEnvironment, Any] = (suite("Kube Tasks")(
    CreateKubeJobTestSuite.spec,
    GetKubeJobTestSuite.spec,
    DeleteKubeJobTestSuite.spec,
    GetKubeJobTestSuite.failing
  ) @@ TestAspect.sequential).provide(env.orDie)
}
