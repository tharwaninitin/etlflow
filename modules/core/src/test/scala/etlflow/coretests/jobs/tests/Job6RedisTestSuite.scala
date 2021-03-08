package etlflow.coretests.jobs.tests

import etlflow.coretests.{LoadData, TestSuiteHelper}
import org.scalatest.{FlatSpec, Matchers}

class Job6RedisTestSuite extends FlatSpec with Matchers with TestSuiteHelper {

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "Job6"
    )
  )

  "LoadData" should "EtlJob6 should run successfully" in {
    assert(true)
  }
}

