package etlflow

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import etlflow.audit.Audit
import etlflow.k8s.K8S
import etlflow.task._
import zio.ULayer
import zio.test._

object RunTests extends ZIOSpecDefault {

  private val env: ULayer[K8S with Jobs with Audit] = K8S.test ++ audit.test

  override def spec: Spec[TestEnvironment, Any] = (suite("Kube Tasks")(
    CreateKubeJobTestSuite.spec,
    GetKubeJobTestSuite.spec @@ TestAspect.ignore,
    TrackKubeJobTestSuite.spec @@ TestAspect.ignore
  ) @@ TestAspect.sequential).provide(env)
}
