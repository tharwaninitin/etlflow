package etlflow.utils

import etlflow.utils.EtlFlowHelper.{EtlJobArgs, Props}
import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec
class RequestValidatorTestSuite  extends AnyFlatSpec with Matchers {

  val actualOutput1 = RequestValidator.validator("Job2LocalJobGenericStep",Some("""("year":"2016","start_date":"2016-03-15")"""))
  val expectedOutput1 = EtlJobArgs("Job2LocalJobGenericStep",List(Props("start_date","2016-03-15"), Props("year","2016")))

  val actualOutput2 = RequestValidator.validator("Job2LocalJobGenericStep",Some("""("refresh_dates":"'202010':'202010'")"""))
  val expectedOutput2 = EtlJobArgs("Job2LocalJobGenericStep",List(Props("refresh_dates","'202010':'202010'")))


  val actualOutput3 = RequestValidator.validator("Job2LocalJobGenericStep",None)
  val expectedOutput3 = EtlJobArgs("Job2LocalJobGenericStep",List.empty)

  "RequestValidator" should " return parsed EtlJobArgs when provided correct get url data" in {
    assert(actualOutput1.right.get == expectedOutput1)
  }

  "RequestValidator" should " return parsed EtlJobArgs when provided correct get url data -1 " in {
    assert(actualOutput2.right.get == expectedOutput2)
  }

  "RequestValidator" should " return parsed EtlJobArgs when provided props as empty" in {
    assert(actualOutput3.right.get == expectedOutput3)
  }

}
