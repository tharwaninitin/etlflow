package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import etlflow.audit.AuditEnv
import etlflow.k8s.K8sEnv
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object GetK8sJobTestSuite {
  val spec: Spec[K8sEnv with Jobs with AuditEnv, Any] =
    test("Execute GetK8sJobTask") {
      val task = GetK8sJobTask(name = "K8sJobTaskExample").execute
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
