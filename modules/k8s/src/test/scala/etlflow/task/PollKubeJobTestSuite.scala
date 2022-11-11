package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object PollKubeJobTestSuite {
  val spec: Spec[K8S with Jobs with Audit, Any] =
    test("Execute GetKubeJobTask") {
      val task = PollKubeJobTask(name = "KubeJobTaskExample").execute
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
