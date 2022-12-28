package etlflow.audit

import zio.ZIO
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object BQTestSuite {
  private val sri       = "a27a7415-57b2-4b53-8f9b-5254e847a4123"
  private val args      = Map("0" -> "arg0", "1" -> "arg1")
  private val jobProps  = Map("key1" -> "value1", "key2" -> "value2")
  private val taskProps = Map("key3" -> "value3", "key4" -> "value4")

  val spec: Spec[Audit, Any] =
    suite("Audit BQ Suite")(
      zio.test.test("logJobStart Test")(
        Audit.logJobStart("Job1", args, jobProps).as(assertCompletes)
      ),
      zio.test.test("logTaskStart Test")(
        Audit.logTaskStart(sri, "Task1", taskProps, "GenericTask").as(assertCompletes)
      ),
      zio.test.test("logTaskEnd Test")(
        Audit.logTaskEnd(sri, "Task1", taskProps, "GenericTask").as(assertCompletes)
      ),
      zio.test.test("logJobEnd Test")(
        Audit.logJobEnd("Job1", args, jobProps).as(assertCompletes)
      ),
      zio.test.test("getJobRuns Test")(
        Audit
          .getJobRuns("SELECT * FROM etlflow.jobrun")
          .tap(op => ZIO.foreach(op)(i => ZIO.logInfo(i.toString)))
          .as(assertCompletes)
      ),
      zio.test.test("getTaskRuns Test")(
        Audit
          .getTaskRuns("SELECT * FROM etlflow.taskrun")
          .tap(op => ZIO.foreach(op)(i => ZIO.logInfo(i.toString)))
          .as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}