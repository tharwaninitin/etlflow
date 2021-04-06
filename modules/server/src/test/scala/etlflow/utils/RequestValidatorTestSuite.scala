package etlflow.utils

import etlflow.utils.EtlFlowHelper.{EtlJobArgs, Props}
import org.scalatest.{FlatSpec, Matchers}

class RequestValidatorTestSuite extends FlatSpec with Matchers {

  "RequestValidator" should " return parsed EtlJobArgs when provided correct params - 1" in {
    val actualOutput1   = RequestValidator("Job2LocalJobGenericStep",Some("""("year":"2016","start_date":"2016-03-15")"""))
    val expectedOutput1 = EtlJobArgs("Job2LocalJobGenericStep",List(Props("year","2016"), Props("start_date","2016-03-15")))
    assert(actualOutput1.right.get == expectedOutput1)
  }

  "RequestValidator" should " return parsed EtlJobArgs when provided correct params - 2" in {
    val actualOutput2   = RequestValidator("Job2LocalJobGenericStep",Some("""("refresh_dates":"'202010':'202010'")"""))
    val expectedOutput2 = EtlJobArgs("Job2LocalJobGenericStep",List(Props("refresh_dates","'202010':'202010'")))
    assert(actualOutput2.right.get == expectedOutput2)
  }

  "RequestValidator" should " return parsed EtlJobArgs when provided props as empty" in {
    val actualOutput3   = RequestValidator("Job2LocalJobGenericStep",None)
    val expectedOutput3 = EtlJobArgs("Job2LocalJobGenericStep",List.empty)
    assert(actualOutput3.right.get == expectedOutput3)
  }

  "RequestValidator" should " return error when provided incorrect props in params" in {
    val actualOutput3 = RequestValidator("Job2LocalJobGenericStep",Some("""("year";"2016")"""))
    assert(actualOutput3.left.get contains "expected")
  }

}
