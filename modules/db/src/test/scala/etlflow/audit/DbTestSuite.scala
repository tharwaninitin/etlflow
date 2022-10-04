package etlflow.audit

import zio.test._

object DbTestSuite {
  val sri = "a27a7415-57b2-4b53-8f9b-5254e847a4123"
  val spec: Spec[TestEnvironment with LogEnv, Any] =
    suite("DB(log) Suite")(
      test("logJobStart Test")(
        LogApi.logJobStart("Job1", "{}", 0L).as(assertCompletes)
      ),
      test("logTaskStart Test")(
        LogApi.logTaskStart(sri, "Task1", Map.empty, "GenericTask", 0L).as(assertCompletes)
      ),
      test("logTaskEnd Test")(
        LogApi.logTaskEnd(sri, "Task1", Map.empty, "GenericTask", 0L).as(assertCompletes)
      ),
      test("logJobEnd Test")(
        LogApi.logJobEnd("Job1", "{}", 0L).as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
