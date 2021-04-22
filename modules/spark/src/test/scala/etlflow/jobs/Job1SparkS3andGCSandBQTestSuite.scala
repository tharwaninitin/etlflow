package etlflow.jobs

import etlflow.coretests.Schema.EtlJob6Props
import etlflow.coretests.TestSuiteHelper
import etlflow.jobs.Job2SparkReadWriteApiTestSuite.testDBLayer
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object Job1SparkS3andGCSandBQTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job 1") {
        val job = Job1SparkS3andGCSandBQSteps(EtlJob6Props())
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ).provideCustomLayerShared(testDBLayer.orDie)
}


