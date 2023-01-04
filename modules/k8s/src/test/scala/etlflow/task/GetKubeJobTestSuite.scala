package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.K8S
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object GetKubeJobTestSuite {

  val spec: Spec[K8S with Audit, Any] =
    test("Execute GetKubeJobTask") {
      val task = GetKubeJobTask("GetKubeJobTask", jobName = jobName, debug = true).execute
      assertZIO(
        task.foldZIO(ex => ZIO.fail(ex.getMessage), status => ZIO.logInfo(status.getStatus.toString) *> ZIO.succeed("ok"))
      )(equalTo("ok"))
    }

  val failing: Spec[K8S with Audit, Any] =
    test("Execute GetKubeJobTask") {
      val task = GetKubeJobTask("GetKubeJobTask", jobName = jobName, debug = true).execute
      assertZIO(
        task.foldZIO(
          ex => ZIO.logInfo(ex.getMessage) *> ZIO.succeed("ok"),
          status => ZIO.logInfo(status.getStatus.toString) *> ZIO.fail("not ok")
        )
      )(equalTo("ok"))
    }
}
