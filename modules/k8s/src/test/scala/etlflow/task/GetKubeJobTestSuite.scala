package etlflow.task

import etlflow.audit.Audit
import etlflow.k8s.Jobs
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object GetKubeJobTestSuite {

  val spec: Spec[Jobs with Audit, Any] =
    test("Execute GetKubeJobTask") {
      val task = GetKubeJobTask(name = jobName, debug = true).execute
      assertZIO(
        task.foldZIO(ex => ZIO.fail(ex.getMessage), status => ZIO.logInfo(status.getStatus.toString) *> ZIO.succeed("ok"))
      )(equalTo("ok"))
    }


  val failing: Spec[Jobs with Audit, Any] =
    test("Execute GetKubeJobTask") {
      val task = GetKubeJobTask(name = jobName, debug = true).execute
      assertZIO(
        task.foldZIO(
          ex => ZIO.logInfo(ex.getMessage) *> ZIO.succeed("ok"),
          status => ZIO.logInfo(status.getStatus.toString) *> ZIO.fail("not ok")
        )
      )(equalTo("ok"))
    }
}
