package etlflow

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import etlflow.audit.AuditEnv
import etlflow.k8s.{K8S, K8sEnv}
import etlflow.task.{CreateKubeJobTestSuite, GetKubeJobTestSuite}
import zio.{TaskLayer, ZLayer}
import zio.test._

object RunTests extends ZIOSpecDefault {

  private val env: TaskLayer[K8sEnv with AuditEnv] = Jobs.test ++ ZLayer.succeed(K8S()) ++ audit.noLog

  override def spec: Spec[TestEnvironment, Any] = (suite("Kube Tasks")(
    CreateKubeJobTestSuite.spec,
    GetKubeJobTestSuite.spec @@ TestAspect.ignore
  ) @@ TestAspect.sequential).provide(env.orDie)
}
