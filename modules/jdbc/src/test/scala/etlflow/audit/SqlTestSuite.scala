package etlflow.audit

import etlflow.DbSuiteHelper
import zio.test._

object SqlTestSuite extends DbSuiteHelper {
  val spec: Spec[Any, Any] =
    suite("SQL(log) Suite")(
      zio.test.test("updateJobRun Sql") {
        val ip = Sql.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "success", "{}").statement
        val op =
          """UPDATE jobrun SET status = ?, props = CAST(? as JSON), updated_at = CURRENT_TIMESTAMP() WHERE job_run_id = ?"""
        assertTrue(ip == op)
      },
      zio.test.test("insertJobRun Sql") {
        val ip = Sql.insertJobRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Job5", "", "").statement
        val op = """INSERT INTO jobrun(
            job_run_id,
            job_name,
            args,
            props,
            status,
            created_at,
            updated_at
            )
         VALUES (?, ?, CAST(? as JSON), CAST(? as JSON), 'started', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""
        assertTrue(ip == op)
      },
      zio.test.test("insertTaskRun Sql") {
        val ip = Sql.insertTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Generic", "{}", "gcp", "123").statement
        val op = """INSERT INTO taskrun (
           task_run_id,
           job_run_id,
           task_name,
           task_type,
           props,
           status,
           created_at,
           updated_at
           )
         VALUES (?, ?, ?, ?, CAST(? as JSON), 'started', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""
        assertTrue(ip == op)
      },
      zio.test.test("updateTaskRun Sql") {
        val ip = Sql
          .updateTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "{}", "success")
          .statement
          .replaceAll("\\s+", " ")
        val op =
          """UPDATE taskrun SET status = ?, props = CAST(? as JSON), updated_at = CURRENT_TIMESTAMP() WHERE task_run_id = ?"""
        assertTrue(ip == op)
      }
    )
}
