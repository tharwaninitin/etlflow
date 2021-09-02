package etlflow.db

import zio.test.Assertion.equalTo
import zio.test._

object SqlTestSuite  {
    val spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("SQL Suite")(
        test("insertJobs Sql")({
            val seq = Seq(
                Seq("Job1", "", "", 0, 0, true),
                Seq("Job2", "", "", 0, 0, true),
            )
            val ip = Sql.insertJobs(seq).statement
            val op = """insert into Job values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?) ON CONFLICT(job_name) DO UPDATE SET schedule = EXCLUDED.schedule"""
            assert(ip)(equalTo(op))
        }),
        test("insertJobs Params")({
            val seq = Seq(
                Seq("Job1", "", "", 0, 0, true),
                Seq("Job2", "", "", 0, 0, true),
            )
            val ip = Sql.insertJobs(seq).parameters
            val op = Seq("Job1", "", "", 0, 0, true,"Job2", "", "", 0, 0, true)
            assert(ip)(equalTo(op))
        }),
        test("selectJobs Sql")({
            val ip = Sql.selectJobs.statement
            val op = """SELECT job_name, schedule, is_active FROM job"""
            assert(ip)(equalTo(op))
        }),
        test("selectJobs Params")({
            val ip = Sql.selectJobs.parameters
            val op = List.empty
            assert(ip)(equalTo(op))
        }),
        test("deleteJobs Sql")({
            val jobDB: List[JobDB] = List(
                JobDB("Job1", "", true)
            )
            val ip = Sql.deleteJobs(jobDB).statement
            val op = """DELETE FROM job WHERE job_name not in (?)"""
            assert(ip)(equalTo(op))
        }),
        test("deleteJobs Params")({
            val jobDB: List[JobDB] = List(
                JobDB("Job1", "", true)
            )
            val ip = Sql.deleteJobs(jobDB).parameters
            val op = List("Job1")
            assert(ip)(equalTo(op))
        }),
        test("updateJobRun Sql")({
            val ip = Sql.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "success", "").statement.replaceAll("\\s+", " ").trim
            val op = """UPDATE JobRun SET state = ?, elapsed_time = ? WHERE job_run_id = ?"""
            assert(ip)(equalTo(op))
        }),
        test("updateJobRun Params")({
            val ip = Sql.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "success", "").parameters
            val op = List("success", "", "a27a7415-57b2-4b53-8f9b-5254e847a301")
            assert(ip)(equalTo(op))
        }),
        test("insertJobRun Sql")({
            val ip = Sql.insertJobRun("a27a7415-57b2-4b53-8f9b-5254e847a30123",
                "Job5",
                "",
                "",
                "",
                0L).statement
            val op = """INSERT INTO JobRun(
            job_run_id,
            job_name,
            properties,
            state,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES (?, ?,  ?, 'started', '...', ?, ?, ?)"""
            assert(ip)(equalTo(op))
        }),
        test("insertStepRun Sql")({
            val ip = Sql.insertStepRun("a27a7415-57b2-4b53-8f9b-5254e847a30123",
                "Generic",
                "{}",
                "gcp",
                "123",
                0L).statement
            val op = """INSERT INTO StepRun (
           job_run_id,
           step_name,
           properties,
           state,
           elapsed_time,
           step_type,
           step_run_id,
           inserted_at
           )
         VALUES (?, ?, ?, 'started', '...', ?, ?, ?)"""
            assert(ip)(equalTo(op))
        }),
        test("updateStepRun Sql")({
            val ip = Sql.updateStepRun("a27a7415-57b2-4b53-8f9b-5254e847a30123",
                "Generic",
                "{}",
                "success",
                "123"
            ).statement.replaceAll("\\s+", " ")
            val op = """UPDATE StepRun SET state = ?, properties = ?, elapsed_time = ? WHERE job_run_id = ? AND step_name = ?"""
            assert(ip)(equalTo(op))
        }),
        test("updateCredentials Sql")({
            val credentialDB = CredentialDB("Sample1","JDBC", JsonString("{}"))
            val ip = Sql.updateCredentials(credentialDB).statement.replaceAll("\\s+", " ").trim
            val op = """UPDATE credential SET valid_to = NOW() - INTERVAL '00:00:01' WHERE credential.name = ? AND credential.valid_to IS NULL"""
            assert(ip)(equalTo(op))
        }),
        test("updateCredentials Params")({
            val credentialDB = CredentialDB("Sample1","JDBC", JsonString("{}"))
            val ip = Sql.updateCredentials(credentialDB).parameters
            assert(ip)(equalTo(List("Sample1")))
        }),
        test("addCredentials Sql")({
            val credentialDB = CredentialDB("Sample2","JDBC", JsonString("{}"))
            val ip = Sql.addCredentials(credentialDB, JsonString("{}")).statement.replaceAll("\\s+", " ").trim
            val op = """INSERT INTO credential (name,type,value) VALUES (?, ?, ?)"""
            assert(ip)(equalTo(op))
        }),
        test("updateJobState Sql")({
            val etlJobStateArgs = EtlJobStateArgs("Job1", true)
            val ip = Sql.updateJobState(etlJobStateArgs).statement
            val op = """UPDATE job SET is_active = ? WHERE job_name = ?"""
            assert(ip)(equalTo(op))
        }),
        test("updateJobState Params")({
            val etlJobStateArgs = EtlJobStateArgs("Job1", true)
            val ip = Sql.updateJobState(etlJobStateArgs).parameters
            assert(ip)(equalTo(List(true,"Job1")))
        }),
        test("updateFailedJob Sql")({
            val ip = Sql.updateFailedJob("Job1",0L).statement
            val op = """UPDATE job SET failed = (failed + 1), last_run_time = ? WHERE job_name = ?"""
            assert(ip)(equalTo(op))
        }),
        test("updateFailedJob Params")({
            val ip = Sql.updateFailedJob("Job1",0L).parameters
            assert(ip)(equalTo(List(0,"Job1")))
        }),
        test("updateSuccessJob Sql")({
            val ip = Sql.updateSuccessJob("Job1",0L).statement
            val op = """UPDATE job SET success = (success + 1), last_run_time = ? WHERE job_name = ?"""
            assert(ip)(equalTo(op))
        }),
        test("updateSuccessJob Params")({
            val ip = Sql.updateSuccessJob("Job1",0L).parameters
            assert(ip)(equalTo(List(0,"Job1")))
        }),
        test("getCredentials Sql")({
            val ip = Sql.getCredentials.statement
            val op = """SELECT name, type::TEXT ,valid_from::TEXT FROM credential WHERE valid_to is null;"""
            assert(ip)(equalTo(op))
        }),
        test("getCredentials Params")({
            val ip = Sql.getCredentials.parameters
            assert(ip)(equalTo(List.empty))
        }),
        test("getJobLogs Sql")({
            val jobLogsArgs =  JobLogsArgs(None,None)
            val ip = Sql.getJobLogs(jobLogsArgs).statement.replaceAll("\\s+", " ").trim
            val op = """SELECT job_name,sum(success)::varchar as success, sum(failed)::varchar as failed from ( SELECT job_name, CASE WHEN state = 'pass' THEN sum(count) ELSE 0 END success, CASE WHEN state != 'pass' THEN sum(count) ELSE 0 END failed FROM (select job_name, state,count(*) as count from jobrun GROUP by job_name,state limit 20) t GROUP by job_name,state ) t1 GROUP by job_name;"""
            assert(ip)(equalTo(op))
        }),
        test("getJobLogs Params")({
            val jobLogsArgs =  JobLogsArgs(None,None)
            val ip = Sql.getJobLogs(jobLogsArgs).parameters
            assert(ip)(equalTo(List.empty))
        }),
        test("getJobRuns Sql")({
            val dbJobRunArgs =  DbJobRunArgs(Some("a27a7415-57b2-4b53-8f9b-5254e847a301"), Some("Job1"), None, None, None, 10, 10)
            val ip = Sql.getJobRuns(dbJobRunArgs).statement.replaceAll("\\s+", " ").trim
            val op = """SELECT job_run_id, job_name, properties::TEXT, state, elapsed_time, job_type, is_master, inserted_at FROM jobRun WHERE is_master = 'true' ORDER BY inserted_at DESC offset ? limit ?"""
            assert(ip)(equalTo(op))
        }),
        test("getJobRuns Params")({
            val dbJobRunArgs =  DbJobRunArgs(Some("a27a7415-57b2-4b53-8f9b-5254e847a301"), Some("Job1"), None, None, None, 10, 10)
            val ip = Sql.getJobRuns(dbJobRunArgs).parameters
            assert(ip)(equalTo(List(10, 10)))
        }),
        test("getStepRuns Sql")({
            val ip = Sql.getStepRuns("a27a7415-57b2-4b53-8f9b-5254e847a301").statement.replaceAll("\\s+", " ").trim
            val op = """SELECT job_run_id, step_name, properties::TEXT, state, elapsed_time, step_type, step_run_id, inserted_at FROM StepRun WHERE job_run_id = ? ORDER BY inserted_at DESC"""
            assert(ip)(equalTo(op))
        }),
        test("getStepRuns Params")({
            val ip = Sql.getStepRuns("a27a7415-57b2-4b53-8f9b-5254e847a301").parameters
            assert(ip)(equalTo(List("a27a7415-57b2-4b53-8f9b-5254e847a301")))
        }),
        test("getJobs Sql")({
            val ip = Sql.getJobs.statement
            val op = """SELECT x.job_name, x.job_description, x.schedule, x.failed, x.success, x.is_active, x.last_run_time FROM job x"""
            assert(ip)(equalTo(op))
        }),
        test("getJobs Params")({
            val ip = Sql.getJobs.parameters
            assert(ip)(equalTo(List.empty))
        }),
        test("getJob Sql")({
            val ip = Sql.getJob("Job1").statement
            val op = """SELECT job_name, schedule, is_active FROM job WHERE job_name = ?"""
            assert(ip)(equalTo(op))
        }),
        test("getJob Params")({
            val ip = Sql.getJob("Job1").parameters
            assert(ip)(equalTo(List("Job1")))
        }),
        test("getCredentialsWithFilter Sql")({
            val ip = Sql.getCredentialsWithFilter("AWS").statement
            val op = """SELECT value FROM credential WHERE name='?' and valid_to is null"""
            assert(ip)(equalTo(op))
        }),
        test("getCredentialsWithFilter Params")({
            val ip = Sql.getCredentialsWithFilter("AWS").parameters
            assert(ip)(equalTo(List("AWS")))
        }),
        test("getUser Sql")({
            val ip = Sql.getUser("admin").statement
            val op = """SELECT user_name, password, user_active, user_role FROM userinfo WHERE user_name = ?"""
            assert(ip)(equalTo(op))
        }),
        test("getUser Params")({
            val ip = Sql.getUser("admin").parameters
            assert(ip)(equalTo(List("admin")))
        }),
    ))
}
