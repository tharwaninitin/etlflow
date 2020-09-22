package etlflow.jobs

import etlflow.Schema.EtlJob1Props
import etlflow.TestSuiteHelper
import org.testcontainers.containers.PostgreSQLContainer
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment, suite, testM}

object Job2SparkReadWriteApiTestSuite extends DefaultRunnableSpec with TestSuiteHelper {
  val container = new PostgreSQLContainer("postgres:latest")
  container.start()

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job 1") {
        val job = Job2SparkReadWriteApi(EtlJob1Props(),global_properties)
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}


