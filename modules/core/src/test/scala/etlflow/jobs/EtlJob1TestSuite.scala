package etlflow.jobs

import etlflow.Schema.EtlJob1Props
import etlflow.TestSuiteHelper
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object EtlJob1TestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job 1") {
        val job = EtlJob1Definition(EtlJob1Props(),global_properties)
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}


