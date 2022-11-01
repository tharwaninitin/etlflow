package etlflow

import etlflow.audit.Audit
import etlflow.k8s.{K8S, K8sEnv}
import etlflow.task.{CreateKubeJobTestSuite, GetKubeJobTestSuite}
import zio.ULayer
import zio.test._

object RunTests extends ZIOSpecDefault {

  private val env: ULayer[K8sEnv with Audit] = K8S.test ++ audit.test

  override def spec: Spec[TestEnvironment, Any] = (suite("Kube Tasks")(
    CreateKubeJobTestSuite.spec,
    GetKubeJobTestSuite.spec @@ TestAspect.ignore
  ) @@ TestAspect.sequential).provide(env)
}
