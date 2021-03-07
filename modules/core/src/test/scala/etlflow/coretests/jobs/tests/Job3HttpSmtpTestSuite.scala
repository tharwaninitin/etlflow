package etlflow.coretests.jobs.tests

import etlflow.coretests.{LoadData, TestSuiteHelper}
import org.scalatest.{FlatSpec, Matchers}

class Job3HttpSmtpTestSuite extends FlatSpec with Matchers with TestSuiteHelper {

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "Job3"
    )
  )

  "LoadData" should "Job3 should run successfully" in {
    assert(true)
  }
}