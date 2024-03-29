package etlflow.audit

import etlflow.DbSuiteHelper
import zio.test._

object SqlTestSuite extends DbSuiteHelper {
  val spec: Spec[Any, Any] =
    suite("Audit SQL Suite")(
      zio.test.test("updateJobRun Sql") {
        val ip = Sql.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "success").statement.replaceAll("\\s+", " ")
        val op = """UPDATE jobrun SET status = ?, updated_at = CURRENT_TIMESTAMP(6) WHERE job_run_id = ?"""
        assertTrue(ip == op)
      },
      zio.test.test("insertJobRun Sql") {
        val ip = Sql.insertJobRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Job5", "", "started").statement
        val op = """INSERT INTO jobrun(
            job_run_id,
            job_name,
            metadata,
            status,
            created_at,
            updated_at
            )
         VALUES (?, ?, CAST(? as JSON), ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))"""
        assertTrue(ip == op)
      },
      zio.test.test("insertTaskRun Sql") {
        val ip = Sql.insertTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Generic", "{}", "gcp", "123", "started").statement
        val op = """INSERT INTO taskrun (
           task_run_id,
           job_run_id,
           task_name,
           task_type,
           metadata,
           status,
           created_at,
           updated_at
           )
         VALUES (?, ?, ?, ?, CAST(? as JSON), ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))"""
        assertTrue(ip == op)
      },
      zio.test.test("updateTaskRun Sql") {
        val ip = Sql.updateTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "success").statement.replaceAll("\\s+", " ")
        val op = """UPDATE taskrun SET status = ?, updated_at = CURRENT_TIMESTAMP(6) WHERE task_run_id = ?"""
        assertTrue(ip == op)
      }
    )
}
