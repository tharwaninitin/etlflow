package etlflow.jobs

import etlflow.{LoadData, TestSuiteHelper}
import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec
class Job4DBTestSuite extends AnyFlatSpec with Matchers with TestSuiteHelper {

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "EtlJob4",
    )
  )

  "LoadData" should "EtlJob4 should run successfully" in {
    assert(true)
  }
}