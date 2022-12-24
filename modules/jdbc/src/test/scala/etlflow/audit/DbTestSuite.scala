package etlflow.audit

import zio.test._

object DbTestSuite {
  val sri = "a27a7415-57b2-4b53-8f9b-5254e847a4123"
  val spec: Spec[Audit, Any] =
    suite("DB(log) Suite")(
      zio.test.test("logJobStart Test")(
        Audit.logJobStart("Job1", Map.empty, Map.empty, 0L).as(assertCompletes)
      ),
      zio.test.test("logTaskStart Test")(
        Audit.logTaskStart(sri, "Task1", Map.empty, "GenericTask", 0L).as(assertCompletes)
      ),
      zio.test.test("logTaskEnd Test")(
        Audit.logTaskEnd(sri, "Task1", Map.empty, "GenericTask", 0L).as(assertCompletes)
      ),
      zio.test.test("logJobEnd Test")(
        Audit.logJobEnd("Job1", Map.empty, Map.empty, 0L).as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
