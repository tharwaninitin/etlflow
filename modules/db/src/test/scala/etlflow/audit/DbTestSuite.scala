package etlflow.audit

import zio.test._

object DbTestSuite {
  val sri = "a27a7415-57b2-4b53-8f9b-5254e847a4123"
  val spec: Spec[TestEnvironment with AuditEnv, Any] =
    suite("DB(log) Suite")(
      test("logJobStart Test")(
        AuditApi.logJobStart("Job1", "{}", 0L).as(assertCompletes)
      ),
      test("logTaskStart Test")(
        AuditApi.logTaskStart(sri, "Task1", Map.empty, "GenericTask", 0L).as(assertCompletes)
      ),
      test("logTaskEnd Test")(
        AuditApi.logTaskEnd(sri, "Task1", Map.empty, "GenericTask", 0L).as(assertCompletes)
      ),
      test("logJobEnd Test")(
        AuditApi.logJobEnd("Job1", "{}", 0L).as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
