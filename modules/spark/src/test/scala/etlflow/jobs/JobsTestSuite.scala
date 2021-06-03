package etlflow.jobs

import etlflow.coretests.Schema.EtlJobDeltaLake
import etlflow.jobs.Job2SparkReadWriteApiTestSuite.testDBLayer
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object JobsTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Jobs") (
      testM("Execute Delta Lake Step") {
        val job = Job4SparkDeltaStep(EtlJobDeltaLake())
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ).provideCustomLayerShared(testDBLayer.orDie)
}
