package etlflow.audit

import scalikejdbc.WrappedResultSet
import zio.ZIO
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object DbTestSuite {
  val sri = "a27a7415-57b2-4b53-8f9b-5254e847a4123"
  val spec: Spec[Audit, Any] =
    suite("Audit DB Suite")(
      zio.test.test("logJobStart Test")(
        Audit.logJobStart("Job1", Map.empty).as(assertCompletes)
      ),
      zio.test.test("logTaskStart Test")(
        Audit.logTaskStart(sri, "Task1", Map.empty, "GenericTask").as(assertCompletes)
      ),
      zio.test.test("logTaskEnd Test")(
        Audit.logTaskEnd(sri, "Task1", Map.empty, "GenericTask").as(assertCompletes)
      ),
      zio.test.test("logJobEnd Test")(
        Audit.logJobEnd("Job1", Map.empty).as(assertCompletes)
      ),
      zio.test.test("getJobRuns Test")(
        Audit
          .getJobRuns("SELECT * FROM jobrun")
          .tap(op => ZIO.foreach(op)(i => ZIO.logInfo(i.toString)))
          .as(assertCompletes)
      ),
      zio.test.test("getTaskRuns Test")(
        Audit
          .getTaskRuns("SELECT * FROM taskrun")
          .tap(op => ZIO.foreach(op)(i => ZIO.logInfo(i.toString)))
          .as(assertCompletes)
      ),
      zio.test.test("fetchResults Test")(
        Audit
          .fetchResults("SELECT job_name FROM jobrun") { case rs: WrappedResultSet =>
            rs.string("job_name")
          }
          .tap(op => ZIO.foreach(op)(i => ZIO.logInfo(i)))
          .as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
