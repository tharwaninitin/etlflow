package etlflow.server

import etlflow.server.model._
import zio.test.{assertTrue, environment, suite, test, ZSpec}

object SqlTestSuite {
  val jobDBs: List[JobDB] = List(
    JobDB("Job1", "", true),
    JobDB("Job2", "", true)
  )
  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("SQL(server) Suite")(
      test("insertJobs Sql") {
        val ip = Sql.insertJobs(jobDBs).statement
        val op =
          """insert into Job values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?) ON CONFLICT(job_name) DO UPDATE SET schedule = EXCLUDED.schedule"""
        assertTrue(ip == op)
      },
      test("insertJobs Params") {
        val ip = Sql.insertJobs(jobDBs).parameters
        assertTrue(ip == Seq("Job1", "", "", 0, 0, true, "Job2", "", "", 0, 0, true))
      },
      test("selectJobs Sql") {
        val ip = Sql.selectJobs.statement
        val op = """SELECT job_name, schedule, is_active FROM job"""
        assertTrue(ip == op)
      },
      test("selectJobs Params") {
        val ip = Sql.selectJobs.parameters
        val op = List.empty
        assertTrue(ip == op)
      },
      test("deleteJobs Sql") {
        val jobDB: List[JobDB] = List(
          JobDB("Job1", "", true)
        )
        val ip = Sql.deleteJobs(jobDB).statement
        val op = """DELETE FROM job WHERE job_name not in (?)"""
        assertTrue(ip == op)
      },
      test("deleteJobs Params") {
        val jobDB: List[JobDB] = List(
          JobDB("Job1", "", true)
        )
        val ip = Sql.deleteJobs(jobDB).parameters
        val op = List("Job1")
        assertTrue(ip == op)
      },
      test("updateCredentials Sql") {
        val credentialDB = Credential("Sample1", "JDBC", "{}")
        val ip           = Sql.updateCredentials(credentialDB).statement.replaceAll("\\s+", " ").trim
        val op =
          """UPDATE credential SET valid_to = NOW() - INTERVAL '00:00:01' WHERE credential.name = ? AND credential.valid_to IS NULL"""
        assertTrue(ip == op)
      },
      test("updateCredentials Params") {
        val credentialDB = Credential("Sample1", "JDBC", "{}")
        val ip           = Sql.updateCredentials(credentialDB).parameters
        assertTrue(ip == List("Sample1"))
      },
      test("addCredentials Sql") {
        val credentialDB = Credential("Sample2", "JDBC", "{}")
        val ip           = Sql.addCredentials(credentialDB).statement.replaceAll("\\s+", " ").trim
        val op           = """INSERT INTO credential (name,type,value) VALUES (?, ?, ?::jsonb)"""
        assertTrue(ip == op)
      },
      test("updateJobState Sql") {
        val etlJobStateArgs = EtlJobStateArgs("Job1", true)
        val ip              = Sql.updateJobState(etlJobStateArgs).statement
        val op              = """UPDATE job SET is_active = ? WHERE job_name = ?"""
        assertTrue(ip == op)
      },
      test("updateJobState Params") {
        val etlJobStateArgs = EtlJobStateArgs("Job1", true)
        val ip              = Sql.updateJobState(etlJobStateArgs).parameters
        assertTrue(ip == List(true, "Job1"))
      },
      test("updateFailedJob Sql") {
        val ip = Sql.updateFailedJob("Job1", 0L).statement
        val op = """UPDATE job SET failed = (failed + 1), last_run_time = ? WHERE job_name = ?"""
        assertTrue(ip == op)
      },
      test("updateFailedJob Params") {
        val ip = Sql.updateFailedJob("Job1", 0L).parameters
        assertTrue(ip == List(0, "Job1"))
      },
      test("updateSuccessJob Sql") {
        val ip = Sql.updateSuccessJob("Job1", 0L).statement
        val op = """UPDATE job SET success = (success + 1), last_run_time = ? WHERE job_name = ?"""
        assertTrue(ip == op)
      },
      test("updateSuccessJob Params") {
        val ip = Sql.updateSuccessJob("Job1", 0L).parameters
        assertTrue(ip == List(0, "Job1"))
      },
      test("getCredentials Sql") {
        val ip = Sql.getCredentials.statement
        val op = """SELECT name, type::TEXT ,valid_from::TEXT FROM credential WHERE valid_to is null;"""
        assertTrue(ip == op)
      },
      test("getCredentials Params") {
        val ip = Sql.getCredentials.parameters
        assertTrue(ip == List.empty)
      },
      test("getJobLogs Sql") {
        val jobLogsArgs = JobLogsArgs(None, None)
        val ip          = Sql.getJobLogs(jobLogsArgs).statement.replaceAll("\\s+", " ").trim
        val op =
          """SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from ( SELECT job_name, CASE WHEN status = 'pass' THEN sum(count) ELSE 0 END success, CASE WHEN status != 'pass' THEN sum(count) ELSE 0 END failed FROM (select job_name, status,count(*) as count from jobrun GROUP by job_name,status limit 20) t GROUP by job_name,status ) t1 GROUP by job_name;"""
        assertTrue(ip == op)
      },
      test("getJobLogs Params") {
        val jobLogsArgs = JobLogsArgs(None, None)
        val ip          = Sql.getJobLogs(jobLogsArgs).parameters
        assertTrue(ip == List.empty)
      },
      test("getJobRuns Sql") {
        val dbJobRunArgs =
          DbJobRunArgs(Some("a27a7415-57b2-4b53-8f9b-5254e847a301"), Some("Job1"), None, None, None, 10, 10)
        val ip = Sql.getJobRuns(dbJobRunArgs).statement.replaceAll("\\s+", " ").trim
        val op =
          """SELECT job_run_id, job_name, properties::TEXT, status, elapsed_time, job_type, is_master, inserted_at FROM jobRun WHERE is_master = 'true' ORDER BY inserted_at DESC offset ? limit ?"""
        assertTrue(ip == op)
      },
      test("getJobRuns Params") {
        val dbJobRunArgs =
          DbJobRunArgs(Some("a27a7415-57b2-4b53-8f9b-5254e847a301"), Some("Job1"), None, None, None, 10, 10)
        val ip = Sql.getJobRuns(dbJobRunArgs).parameters
        assertTrue(ip == List(10, 10))
      },
      test("getStepRuns Sql") {
        val ip = Sql.getStepRuns("a27a7415-57b2-4b53-8f9b-5254e847a301").statement.replaceAll("\\s+", " ").trim
        val op =
          """SELECT job_run_id, step_name, properties::TEXT, status, elapsed_time, step_type, step_run_id, inserted_at FROM StepRun WHERE job_run_id = ? ORDER BY inserted_at DESC"""
        assertTrue(ip == op)
      },
      test("getStepRuns Params") {
        val ip = Sql.getStepRuns("a27a7415-57b2-4b53-8f9b-5254e847a301").parameters
        assertTrue(ip == List("a27a7415-57b2-4b53-8f9b-5254e847a301"))
      },
      test("getJobs Sql") {
        val ip = Sql.getJobs.statement
        val op =
          """SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active, x.last_run_time FROM job x"""
        assertTrue(ip == op)
      },
      test("getJobs Params") {
        val ip = Sql.getJobs.parameters
        assertTrue(ip == List.empty)
      },
      test("getJob Sql") {
        val ip = Sql.getJob("Job1").statement
        val op = """SELECT job_name, schedule, is_active FROM job WHERE job_name = ?"""
        assertTrue(ip == op)
      },
      test("getJob Params") {
        val ip = Sql.getJob("Job1").parameters
        assertTrue(ip == List("Job1"))
      },
      test("getCredentialsWithFilter Sql") {
        val ip = Sql.getCredentialsWithFilter("AWS").statement
        val op = """SELECT value FROM credential WHERE name='?' and valid_to is null"""
        assertTrue(ip == op)
      },
      test("getCredentialsWithFilter Params") {
        val ip = Sql.getCredentialsWithFilter("AWS").parameters
        assertTrue(ip == List("AWS"))
      },
      test("getUser Sql") {
        val ip = Sql.getUser("admin").statement
        val op = """SELECT user_name, password, user_active, user_role FROM userinfo WHERE user_name = ?"""
        assertTrue(ip == op)
      },
      test("getUser Params") {
        val ip = Sql.getUser("admin").parameters
        assertTrue(ip == List("admin"))
      }
    )
}
