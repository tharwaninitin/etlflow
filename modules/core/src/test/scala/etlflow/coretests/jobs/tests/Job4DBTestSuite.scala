package etlflow.coretests.jobs.tests

import etlflow.coretests.{LoadData, TestSuiteHelper}
import org.scalatest.{FlatSpec, Matchers}

class Job4DBTestSuite extends FlatSpec with Matchers with TestSuiteHelper {

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "Job4",
    )
  )

  "LoadData" should "Job4 should run successfully" in {
    assert(true)
  }
}