package etlflow.jobs

import etlflow.Schema.EtlJob2Props
import etlflow.TestSuiteHelper
import etlflow.utils.JDBC
import org.testcontainers.containers.PostgreSQLContainer
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object EtlJob2TestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      testM("Execute Etl Job 2") {
        val container = new PostgreSQLContainer("postgres:latest")
        container.start()
        val props = EtlJob2Props(ratings_output_type = JDBC(container.getJdbcUrl, container.getUsername, container.getPassword, global_props.log_db_driver))
        val job = EtlJob2Definition(props,Some(global_props))
        assertM(job.execute().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}


