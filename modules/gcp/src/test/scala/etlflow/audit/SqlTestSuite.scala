package etlflow.audit

import zio.test._

object SqlTestSuite {
  val spec: Spec[Any, Any] =
    suite("Audit SQL Suite")(
      zio.test.test("updateJobRun Sql") {
        val ip = Sql.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "success").replaceAll("\\s+", " ")
        val op =
          """UPDATE etlflow.jobrun SET status = "success", updated_at = CURRENT_TIMESTAMP() WHERE job_run_id = "a27a7415-57b2-4b53-8f9b-5254e847a301""""
        assertTrue(ip == op)
      },
      zio.test.test("insertJobRun Sql") {
        val ip = Sql.insertJobRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Job5", "{}", "started")
        val op = """INSERT INTO etlflow.jobrun(
            job_run_id,
            job_name,
            metadata,
            status,
            created_at,
            updated_at
            )
         VALUES ("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Job5", JSON '{}', "started", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""
        assertTrue(ip == op)
      },
      zio.test.test("insertTaskRun Sql") {
        val ip = Sql.insertTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "Generic", "{}", "BQQuery", "123", "started")
        val op = """INSERT INTO etlflow.taskrun (
           task_run_id,
           job_run_id,
           task_name,
           task_type,
           metadata,
           status,
           created_at,
           updated_at
           )
         VALUES ("a27a7415-57b2-4b53-8f9b-5254e847a30123", "123", "Generic", "BQQuery", JSON '{}', "started", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"""
        assertTrue(ip == op)
      },
      zio.test.test("updateTaskRun Sql") {
        val ip = Sql.updateTaskRun("a27a7415-57b2-4b53-8f9b-5254e847a30123", "success").replaceAll("\\s+", " ")
        val op =
          """UPDATE etlflow.taskrun SET status = "success", updated_at = CURRENT_TIMESTAMP() WHERE task_run_id = "a27a7415-57b2-4b53-8f9b-5254e847a30123""""
        assertTrue(ip == op)
      }
    )
}
