package etlflow.jobs

import etlflow.Schema.EtlJob1Props
import etlflow.TestSuiteHelper
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object Job1SparkS3andGCSandBQTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job 1") {
        val job = Job1SparkS3andGCSandBQSteps(EtlJob1Props(),global_properties)
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}


