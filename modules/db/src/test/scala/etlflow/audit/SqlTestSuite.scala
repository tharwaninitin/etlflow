package etlflow.audit

import etlflow.db.utils
import zio.test._

object SqlTestSuite {
  val spec: Spec[Any, Any] =
    suite("SQL(log) Suite")(
      zio.test.test("updateJobRun Sql") {
        val ipsql = Sql.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "success", "2 mins")
        val ip    = utils.getSqlQueryAsString(ipsql).replaceAll("\\s+", " ").trim
        val op =
          """UPDATE jobrun SET status = success, elapsed_time = 2 mins WHERE job_run_id = a27a7415-57b2-4b53-8f9b-5254e847a301"""
        assertTrue(ip == op)
      },
      zio.test.test("insertJobRun Sql") {
        val ip = Sql.insertJobRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Job5", "", 0L).statement
        val op = """INSERT INTO jobrun(
            job_run_id,
            job_name,
            properties,
            status,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES (?, ?, CAST(? as JSON), 'started', '...', '', 'true', ?)"""
        assertTrue(ip == op)
      },
      zio.test.test("insertTaskRun Sql") {
        val ip = Sql.insertTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Generic", "{}", "gcp", "123", 0L).statement
        val op = """INSERT INTO taskrun (
           task_run_id,
           task_name,
           properties,
           status,
           elapsed_time,
           task_type,
           job_run_id,
           inserted_at
           )
         VALUES (?, ?, CAST(? as JSON), 'started', '...', ?, ?, ?)"""
        assertTrue(ip == op)
      },
      zio.test.test("updateTaskRun Sql") {
        val ip = Sql
          .updateTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "{}", "success", "123")
          .statement
          .replaceAll("\\s+", " ")
        val op = """UPDATE taskrun SET status = ?, properties = CAST(? as JSON), elapsed_time = ? WHERE task_run_id = ?"""
        assertTrue(ip == op)
      }
    )
}
