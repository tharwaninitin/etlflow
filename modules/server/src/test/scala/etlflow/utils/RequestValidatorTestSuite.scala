package etlflow.utils

import etlflow.api.Schema.{EtlJobArgs, Props}
import etlflow.json.JsonEnv
import zio.ZIO
import zio.test.Assertion.{containsString, equalTo}
import zio.test._

object RequestValidatorTestSuite {

  val spec: ZSpec[environment.TestEnvironment with JsonEnv, Any] =
    suite("RequestValidator")(
      testM("should return parsed EtlJobArgs when provided correct params - 1") {
        val actualOutput1   = RequestValidator("Job2LocalJobGenericStep",Some("""("year":"2016","start_date":"2016-03-15")"""))
        val expectedOutput1 = EtlJobArgs("Job2LocalJobGenericStep",Some(List(Props("year","2016"), Props("start_date","2016-03-15"))))
        assertM(actualOutput1.foldM(ex => ZIO.fail(ex.getMessage), etlJobArgs => ZIO.succeed(etlJobArgs)))(equalTo(expectedOutput1))
      },
      testM("should return parsed EtlJobArgs when provided correct params - 2") {
        val actualOutput2   = RequestValidator("Job2LocalJobGenericStep",Some("""("refresh_dates":"'202010':'202010'")"""))
        val expectedOutput2 = EtlJobArgs("Job2LocalJobGenericStep",Some(List(Props("refresh_dates","'202010':'202010'"))))
        assertM(actualOutput2.foldM(ex => ZIO.fail(ex.getMessage), etlJobArgs => ZIO.succeed(etlJobArgs)))(equalTo(expectedOutput2))
      },
      testM("return parsed EtlJobArgs when provided props as empty") {
        val actualOutput3   = RequestValidator("Job2LocalJobGenericStep",None)
        val expectedOutput3 = EtlJobArgs("Job2LocalJobGenericStep")
        assertM(actualOutput3.foldM(ex => ZIO.fail(ex.getMessage), etlJobArgs => ZIO.succeed(etlJobArgs)))(equalTo(expectedOutput3))
      },
      testM("should return error when provided incorrect props in params") {
        val actualOutput3 = RequestValidator("Job2LocalJobGenericStep",Some("""("year";"2016")"""))
        assertM(actualOutput3.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(containsString("expected"))
      },
    )
}
