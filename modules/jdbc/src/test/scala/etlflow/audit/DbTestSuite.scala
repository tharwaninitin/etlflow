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
        Audit.logJobStart("Job1", "{}").as(assertCompletes)
      ),
      zio.test.test("logTaskStart Test")(
        Audit.logTaskStart(sri, "Task1", "{}", "GenericTask").as(assertCompletes)
      ),
      zio.test.test("logTaskEnd Test")(
        Audit.logTaskEnd(sri).as(assertCompletes)
      ),
      zio.test.test("logJobEnd Test")(
        Audit.logJobEnd().as(assertCompletes)
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
          .fetchResults[WrappedResultSet, String]("SELECT job_name FROM jobrun") { rs =>
            rs.string("job_name")
          }
          .tap(op => ZIO.foreach(op)(i => ZIO.logInfo(i)))
          .as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
