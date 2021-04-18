package etlflow.jdbc

import cats.effect.{ContextShift, IO}
import doobie.util.{ExecutionContexts, log}
import doobie.util.transactor.Transactor
import etlflow.ServerSuiteHelper
import etlflow.api.Schema._
import org.scalatest._
import etlflow.log.{JobRun, StepRun}
import java.time.LocalDate

class DoobieTestSuite extends funsuite.AnyFunSuite with matchers.should.Matchers with doobie.scalatest.IOChecker with ServerSuiteHelper {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContexts.synchronous)
  implicit val dbLogger: log.LogHandler = DBLogger()
  val transactor: doobie.Transactor[IO] = Transactor.fromDriverManager[IO](credentials.driver, credentials.url, credentials.user, credentials.password)

  val creds: CredentialDB = CredentialDB("test", "jdbc", JsonString("{}"))
  val jobs = List(EtlJob("Job6", Map.empty),EtlJob("Job5", Map.empty))
  val jobsDB = jobs.map{x =>
    JobDB(x.name, x.props.getOrElse("job_schedule",""), is_active = true)
  }
  val etlJobStateArgs = EtlJobStateArgs("Job6",true)
  val dbStepRunArgs  = DbStepRunArgs("12345")
  val dbJobRunArgs1 = DbJobRunArgs(jobRunId = Some("12345"),
    jobName = None,
    startTime = None,
    endTime = None,
    filter = None,
    limit = 10,
    offset = 10)

  val dbJobRunArgs2 = DbJobRunArgs(jobRunId = None,
    jobName = Some("Job6"),
    startTime = Some(LocalDate.now()),
    endTime = Some(LocalDate.now()),
    filter = Option("IN"),
    limit = 10,
    offset = 10)

  val dbJobRunArgs3 = DbJobRunArgs(jobRunId = None,
    jobName = Some("Job6"),
    startTime = Some(LocalDate.now()),
    endTime = Some(LocalDate.now()),
    filter = Option("NOT IN"),
    limit = 0,
    offset = 0)

  val dbJobRunArgs4 = DbJobRunArgs(jobRunId = None,
    jobName = Some("Job6"),
    startTime = None,
    endTime = None,
    filter = Option("NOT IN"),
    limit = 0,
    offset = 0)

  val dbJobRunArgs5 = DbJobRunArgs(jobRunId = None,
    jobName = Some("Job6"),
    startTime = None,
    endTime = None,
    filter = Option("IN"),
    limit = 0,
    offset = 0)

  val dbJobRunArgs6 = DbJobRunArgs(jobRunId = None,
    jobName = None,
    startTime = Some(LocalDate.now()),
    endTime = Some(LocalDate.now()),
    filter = None,
    limit = 0,
    offset = 0)

  val dbJobRunArgs7 = DbJobRunArgs(jobRunId = None,
    jobName = None,
    startTime = None,
    endTime = None,
    filter = None,
    limit = 0,
    offset = 0)

  val jobLogsArgs1 = JobLogsArgs (
    filter = Some(10),
    limit = Some(10)
  )

  val jobLogsArgs2 = JobLogsArgs (
    filter = Some(10),
    limit = None
  )

  val jobLogsArgs3 = JobLogsArgs (
    filter = None,
    limit =  Some(10)
  )

  val jobLogsArgs4 = JobLogsArgs (
    filter = None,
    limit =  None
  )

  val query1: doobie.Update0 = SQL.addCredentials(creds)
    test("addCredentials") { check(query1) }

  val query6: doobie.Update0 = SQL.updateCredentials(creds)
  test("updateCredentials") { check(query6) }

  val query24:doobie.Query0[GetCredential]  = SQL.getCredentials
  test("getCredentials") { check(query24)}

  val query4: doobie.Update0 = SQL.deleteJobs(jobsDB)
  test("deleteJobs") { check(query4) }

  val query41: doobie.Update[JobDB] = SQL.insertJobs
  test("insertJobs") { check(query41) }

  val query7:doobie.Query0[JobDB]  = SQL.selectJobs
  test("selectJobs") { check(query7)}

  val query2: doobie.Update0 = SQL.updateSuccessJob("Job6",123455678)
  test("updateSuccessJob") { check(query2) }

  val query3: doobie.Update0 = SQL.updateFailedJob("Job6",123455678)
  test("updateFailedJob") { check(query3) }

  val query5: doobie.Update0 = SQL.updateJobState(etlJobStateArgs)
  test("updateJobState") { check(query5) }

  val query8:doobie.Query0[UserDB]  = SQL.getUser("admin")
  test("getUser") { check(query8)}

  val query10:doobie.Query0[JobDB]  = SQL.getJob("Job6")
  test("getJob") { check(query10)}

  val query11:doobie.Query0[JobDBAll]  = SQL.getJobs
  test("getJobs") { check(query11)}

  val query12:doobie.Query0[StepRun]  = SQL.getStepRuns(dbStepRunArgs)
  test("getStepRuns") { check(query12)}

  val query13:doobie.Query0[JobRun]  = SQL.getJobRuns(dbJobRunArgs1)
  test("getJobRuns_case1_defined(job_run_id)") { check(query13)}

  val query14:doobie.Query0[JobRun]  = SQL.getJobRuns(dbJobRunArgs2)
  test("getJobRuns_case2_defined(job_name,start_time,end_time,filter(IN))") { check(query14)}

  val query15:doobie.Query0[JobRun]  = SQL.getJobRuns(dbJobRunArgs3)
  test("getJobRuns_case3_defined(job_name,start_time,end_time,filter(NOT IN))") { check(query15)}

  val query16:doobie.Query0[JobRun]  = SQL.getJobRuns(dbJobRunArgs4)
  test("getJobRuns_case_4_defined(job_name,filter(NOT IN))") { check(query16)}

  val query17:doobie.Query0[JobRun]  = SQL.getJobRuns(dbJobRunArgs5)
  test("getJobRuns_case5_defined(job_name,filter(IN))") { check(query17)}

  val query18:doobie.Query0[JobRun]  = SQL.getJobRuns(dbJobRunArgs6)
  test("getJobRuns_case6_defined(start_time,end_time)") { check(query18)}

  val query19:doobie.Query0[JobRun]  = SQL.getJobRuns(dbJobRunArgs7)
  test("getJobRuns_case7_default") { check(query19)}

  val query20:doobie.Query0[JobLogs]  = SQL.getJobLogs(jobLogsArgs1)
  test("getJobLogs_case1_defined(filter,limit)") { check(query20)}

  val query21:doobie.Query0[JobLogs]  = SQL.getJobLogs(jobLogsArgs2)
  test("getJobLogs_case2_defined(filter)") { check(query21)}

  val query22:doobie.Query0[JobLogs]  = SQL.getJobLogs(jobLogsArgs3)
  test("getJobLogs_case3_defined(limit)") { check(query22)}

  val query23:doobie.Query0[JobLogs]  = SQL.getJobLogs(jobLogsArgs4)
  test("getJobLogs_case_default") { check(query23)}

}
