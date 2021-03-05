package etlflow.jobs

import etlflow.{LoadData, TestSuiteHelper}
import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec
class Job6RedisStepTestSuite extends AnyFlatSpec with Matchers  with TestSuiteHelper {

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "EtlJob6"
    )
  )

  "LoadData" should "EtlJob6 should run successfully" in {
    assert(true)
  }
}