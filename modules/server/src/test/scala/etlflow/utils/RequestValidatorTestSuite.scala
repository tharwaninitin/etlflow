package etlflow.utils

import etlflow.utils.EtlFlowHelper.{EtlJobArgs, Props}
import zio.test.DefaultRunnableSpec
import zio.test._
import zio.test.Assertion.{equalTo, containsString}

object RequestValidatorTestSuite extends DefaultRunnableSpec {

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("RequestValidator")(
      test("should return parsed EtlJobArgs when provided correct params - 1") {
        val actualOutput1   = RequestValidator("Job2LocalJobGenericStep",Some("""("year":"2016","start_date":"2016-03-15")"""))
        val expectedOutput1 = EtlJobArgs("Job2LocalJobGenericStep",Some(List(Props("year","2016"), Props("start_date","2016-03-15"))))
        assert(actualOutput1.right.get)(equalTo(expectedOutput1))
      },
      test("should return parsed EtlJobArgs when provided correct params - 2") {
        val actualOutput2   = RequestValidator("Job2LocalJobGenericStep",Some("""("refresh_dates":"'202010':'202010'")"""))
        val expectedOutput2 = EtlJobArgs("Job2LocalJobGenericStep",Some(List(Props("refresh_dates","'202010':'202010'"))))
        assert(actualOutput2.right.get)(equalTo(expectedOutput2))
      },
      test("return parsed EtlJobArgs when provided props as empty") {
        val actualOutput3   = RequestValidator("Job2LocalJobGenericStep",None)
        val expectedOutput3 = EtlJobArgs("Job2LocalJobGenericStep")
        assert(actualOutput3.right.get)(equalTo(expectedOutput3))
      },
      test("should return error when provided incorrect props in params") {
        val actualOutput3 = RequestValidator("Job2LocalJobGenericStep",Some("""("year";"2016")"""))
        assert(actualOutput3.left.get)(containsString("expected"))
      },
    )
}
