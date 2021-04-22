package etlflow.jobs

import etlflow.coretests.Schema.EtlJob2Props
import etlflow.coretests.TestSuiteHelper
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment, suite, testM}

object Job2SparkReadWriteApiTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job 1") {
        val job = Job2SparkReadWriteApi(EtlJob2Props())
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ).provideCustomLayerShared(testDBLayer.orDie)
}


