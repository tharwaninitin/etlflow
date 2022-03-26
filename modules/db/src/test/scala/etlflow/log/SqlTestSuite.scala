package etlflow.log

import etlflow.db.utils
import zio.test._

object SqlTestSuite {
  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("SQL(log) Suite")(
      test("updateJobRun Sql") {
        val ipsql = Sql.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "success", "2 mins")
        val ip    = utils.getSqlQueryAsString(ipsql).replaceAll("\\s+", " ").trim
        val op =
          """UPDATE JobRun SET status = success, elapsed_time = 2 mins WHERE job_run_id = a27a7415-57b2-4b53-8f9b-5254e847a301"""
        assertTrue(ip == op)
      },
      test("insertJobRun Sql") {
        val ip = Sql.insertJobRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Job5", "", 0L).statement
        val op = """INSERT INTO JobRun(
            job_run_id,
            job_name,
            properties,
            status,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES (?, ?, ?::jsonb, 'started', '...', '', 'true', ?)"""
        assertTrue(ip == op)
      },
      test("insertStepRun Sql") {
        val ip = Sql.insertStepRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Generic", "{}", "gcp", "123", 0L).statement
        val op = """INSERT INTO StepRun (
           step_run_id,
           step_name,
           properties,
           status,
           elapsed_time,
           step_type,
           job_run_id,
           inserted_at
           )
         VALUES (?, ?, ?::jsonb, 'started', '...', ?, ?, ?)"""
        assertTrue(ip == op)
      },
      test("updateStepRun Sql") {
        val ip = etlflow.log.Sql
          .updateStepRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "{}", "success", "123")
          .statement
          .replaceAll("\\s+", " ")
        val op = """UPDATE StepRun SET status = ?, properties = ?::jsonb, elapsed_time = ? WHERE step_run_id = ?"""
        assertTrue(ip == op)
      }
    )
}
