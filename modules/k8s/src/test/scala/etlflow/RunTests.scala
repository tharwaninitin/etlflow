package etlflow

import etlflow.audit.Audit
import etlflow.k8s.{Jobs, K8S}
import etlflow.task._
import zio.TaskLayer
import zio.test._

object RunTests extends ZIOSpecDefault {

  private val env: TaskLayer[Jobs with Audit] = K8S.batchClient() ++ audit.test

  override def spec: Spec[TestEnvironment, Any] = (suite("Kube Tasks")(
    CreateKubeJobTestSuite.spec,
    GetKubeJobTestSuite.spec,
    DeleteKubeJobTestSuite.spec,
    GetKubeJobTestSuite.failing
  ) @@ TestAspect.sequential).provide(env.orDie)
}
