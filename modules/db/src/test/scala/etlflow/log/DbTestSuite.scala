package etlflow.log

import zio.test._

object DbTestSuite {
  val sri = "a27a7415-57b2-4b53-8f9b-5254e847a4123"
  val spec: ZSpec[environment.TestEnvironment with LogEnv, Any] =
    suite("DB(log) Suite")(
      testM("logJobStart Test")(
        LogApi.logJobStart("Job1", "{}", 0L).as(assertCompletes)
      ),
      testM("logStepStart Test")(
        LogApi.logStepStart(sri, "Step1", Map.empty, "GenericStep", 0L).as(assertCompletes)
      ),
      testM("logStepEnd Test")(
        LogApi.logStepEnd(sri, "Step1", Map.empty, "GenericStep", 0L).as(assertCompletes)
      ),
      testM("logJobEnd Test")(
        LogApi.logJobEnd("Job1", "{}", 0L).as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
