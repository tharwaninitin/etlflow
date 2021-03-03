package etlflow.utils

import etlflow.webserver.api.TestEtlFlowService
import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import zio._
class QueueHelperTestSuite extends AnyFlatSpec with Matchers with TestEtlFlowService {

  val jobTestQueue1 = Runtime.default.unsafeRun(Queue.unbounded[(String,String)])

  val jobQueue = for {
    jobQueue       <- jobTestQueue1.offer(("EtlJob4","TESTING"))
  } yield  jobQueue

  val actual_job_list_output = Runtime.default.unsafeRun(jobTestQueue1.takeAll)
  val expected_job_list_output = ""

  println("actual_job_list_output :" + actual_job_list_output)

  "Queue" should " return expected output" in {
    assert(actual_job_list_output == expected_job_list_output)
  }

}
