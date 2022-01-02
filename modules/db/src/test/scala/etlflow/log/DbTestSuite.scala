package etlflow.log

import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DbTestSuite {
  val sri = "a27a7415-57b2-4b53-8f9b-5254e847a4123"
  val spec: ZSpec[environment.TestEnvironment with LogEnv, Any] =
    suite("DB(log) Suite")(
      testM("logJobStart Test")(
        assertM(LogApi.logJobStart("Job1", "{}", 0L).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("logStepStart Test")(
        assertM(LogApi.logStepStart(sri, "Step1", Map.empty, "GenericStep", 0L).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("logStepEnd Test")(
        assertM(LogApi.logStepEnd(sri, "Step1", Map.empty, "GenericStep", 0L).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("logJobEnd Test")(
        assertM(LogApi.logJobEnd("Job1", "{}",  0L).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      )
    ) @@ TestAspect.sequential
}