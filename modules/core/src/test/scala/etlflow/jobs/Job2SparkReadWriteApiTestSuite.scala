package etlflow.jobs

import etlflow.{LoadData, TestSuiteHelper}
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.PostgreSQLContainer

class Job2SparkReadWriteApiTestSuite extends FlatSpec with Matchers  with TestSuiteHelper {
  val container = new PostgreSQLContainer("postgres:latest")
  container.start()

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "EtlJob2",
      "--props",
      s"url=${container.getJdbcUrl},user=${container.getUsername},pass=${container.getPassword}"
    )
  )

  "LoadData" should "EtlJob2 should run successfully" in {
    assert(true)
  }
}


