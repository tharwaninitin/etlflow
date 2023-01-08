package etlflow.task

import etlflow.TestHelper
import etlflow.audit.Audit
import gcp4zio.dp._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object DPCreateTestSuite extends TestHelper {
  val spec: Spec[DPCluster with Audit, Any] =
    test("Execute DPCreateTask") {
      val dpProps = ClusterProps(
        bucketName = dpBucket,
        subnetUri = dpSubnetUri,
        networkTags = dpNetworkTags,
        serviceAccount = dpServiceAccount
      )
      val task = DPCreateTask("DPCreateTaskExample", dpCluster, dpProps).execute
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
