package etlflow.jobs

import etlflow.{LoadData, TestSuiteHelper}
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.PostgreSQLContainer

class EtlJob4TestSuite extends FlatSpec with Matchers  with TestSuiteHelper {

  val container = new PostgreSQLContainer("postgres:latest")
  container.start()

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "EtlJob4",
      "--props",
      s"url=${container.getJdbcUrl},user=${container.getUsername},pass=${container.getPassword}"
    )
  )

  "LoadData" should "EtlJob4 should run successfully" in {
    assert(true)
  }
}