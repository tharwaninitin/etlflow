package etlflow.jobs

import etlflow.{LoadData, TestSuiteHelper}
import org.scalatest.{FlatSpec, Matchers}

class Job3HttpSmtpTestSuite extends FlatSpec with Matchers  with TestSuiteHelper {

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "EtlJob3"
    )
  )

  "LoadData" should "EtlJob3 should run successfully" in {
    assert(true)
  }
}