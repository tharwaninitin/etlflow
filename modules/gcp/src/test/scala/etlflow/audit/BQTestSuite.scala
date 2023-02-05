package etlflow.audit

import com.google.cloud.bigquery.FieldValueList
import etlflow.json.JSON
import zio.ZIO
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object BQTestSuite {
  private val sri = "a27a7415-57b2-4b53-8f9b-5254e847a4123"

  private val jobProps = JSON.convertToString(Map("key1" -> "value1", "key2" -> "value2"))

  private val taskProps = JSON.convertToString(Map("key3" -> "value3", "key4" -> "value4"))

  val spec: Spec[Audit, Any] =
    suite("Audit BQ Suite")(
      zio.test.test("logJobStart Test")(
        Audit.logJobStart("Job1", jobProps).as(assertCompletes)
      ),
      zio.test.test("logTaskStart Test")(
        Audit.logTaskStart(sri, "Task1", taskProps, "GenericTask").as(assertCompletes)
      ),
      zio.test.test("logTaskEnd Test")(
        Audit.logTaskEnd(sri).as(assertCompletes)
      ),
      zio.test.test("logJobEnd Test")(
        Audit.logJobEnd().as(assertCompletes)
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
      ),
      zio.test.test("fetchResults Test")(
        Audit
          .fetchResults("SELECT job_name FROM etlflow.jobrun") { case rs: FieldValueList =>
            rs.get("job_name").getStringValue
          }
          .tap(op => ZIO.foreach(op)(i => ZIO.logInfo(i)))
          .as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
