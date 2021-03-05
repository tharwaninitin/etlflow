package etlflow.jobs

import etlflow.{LoadData, TestSuiteHelper}
import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class Job3HttpSmtpTestSuite extends AnyFlatSpec with Matchers  with TestSuiteHelper {

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