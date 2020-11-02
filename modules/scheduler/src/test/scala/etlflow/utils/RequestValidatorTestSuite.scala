package etlflow.utils

import etlflow.utils.EtlFlowHelper.{EtlJobArgs, Props}
import org.scalatest.{FlatSpec, Matchers}

class RequestValidatorTestSuite  extends FlatSpec with Matchers {


  val data1 = """job_name=Job2LocalJobGenericStep&props={"year":"2016","start_date":"2016-03-15"}""".stripMargin
  val data2 = """dag_id=Job2LocalJobGenericStep&props={"year":"2016","start_date":"2016-03-15"}""".stripMargin
  val data3 = """props={"year":"2016","start_date":"2016-03-15"}&job_name=Job2LocalJobGenericStep""".stripMargin
  val data4 = ""
  val actualOutput1 = RequestValidator.validator(data1)
  val expectedOutput1 = EtlJobArgs("Job2LocalJobGenericStep",List(Props("start_date","2016-03-15"), Props("year","2016")))

  val actualOutput2 = RequestValidator.validator(data2)
  val expectedOutput2 = "Invalid Request"

  val actualOutput3 = RequestValidator.validator(data3)
  val expectedOutput3 = EtlJobArgs("Job2LocalJobGenericStep",List(Props("start_date","2016-03-15"), Props("year","2016")))

  val actualOutput4 = RequestValidator.validator(data4)
  val expectedOutput4 = "Invalid Request"

  "RequestValidator" should " return parsed EtlJobArgs when provided correct get url data" in {
    assert(actualOutput1.right.get == expectedOutput1)
  }

  "RequestValidator" should " return parsed invalid request when provided incorrect get url data" in {
    assert(actualOutput2.left.get == expectedOutput2)
  }

  "RequestValidator" should " return parsed EtlJobArgs when provided modified get url data" in {
    assert(actualOutput3.right.get == expectedOutput3)
  }

  "RequestValidator" should " return invalid request when provided empty get url data" in {
    assert(actualOutput4.left.get == expectedOutput4)
  }

}
