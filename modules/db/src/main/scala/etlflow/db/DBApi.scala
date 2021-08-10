package etlflow.db

import doobie.util.Read
import scalikejdbc.WrappedResultSet
import zio.{IO, ZIO}

private[etlflow] object DBApi {
  // Uncomment this to see generated SQL queries in logs
  // implicit val dbLogger = DoobieQueryLogger()

  trait Service {
    def getUser(user_name: String): IO[Throwable, UserDB]
    def getJob(name: String): IO[Throwable, JobDB]
    def getJobs: IO[Throwable, List[JobDBAll]]
    def getStepRuns(args: DbStepRunArgs): IO[Throwable, List[StepRun]]
    def getJobRuns(args: DbJobRunArgs): IO[Throwable, List[JobRun]]
    def getJobLogs(args: JobLogsArgs): IO[Throwable, List[JobLogs]]
    def getCredentials: IO[Throwable, List[GetCredential]]
    def updateSuccessJob(job: String, ts: Long): IO[Throwable, Long]
    def updateFailedJob(job: String, ts: Long): IO[Throwable, Long]
    def updateJobState(args: EtlJobStateArgs): IO[Throwable, Boolean]
    def addCredential(credentialsDB: CredentialDB, actualSerializerOutput: JsonString): IO[Throwable, Credentials]
    def updateCredential(credentialsDB: CredentialDB, actualSerializerOutput: JsonString): IO[Throwable, Credentials]
    def refreshJobs(jobs: List[EtlJob]): IO[Throwable, List[JobDB]]
    def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[Throwable, Unit]
    def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[Throwable, Unit]
    def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[Throwable, Unit]
    def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[Throwable, Unit]
    def executeQueryWithResponse[T <: Product : Read](query: String): IO[Throwable, List[T]]
    def executeQuery(query: String): IO[Throwable, Unit]
    def executeQueryWithSingleResponse[T : Read](query: String): IO[Throwable, T]
    def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): IO[Throwable, T] = ???
  }

  def getUser(user_name: String): ZIO[DBEnv, Throwable, UserDB] = ZIO.accessM(_.get.getUser(user_name))
  def getJob(name: String): ZIO[DBEnv, Throwable, JobDB] = ZIO.accessM(_.get.getJob(name))
  def getJobs: ZIO[DBEnv, Throwable, List[JobDBAll]] = ZIO.accessM(_.get.getJobs)
  def getStepRuns(args: DbStepRunArgs): ZIO[DBEnv, Throwable, List[StepRun]] = ZIO.accessM(_.get.getStepRuns(args))
  def getJobRuns(args: DbJobRunArgs): ZIO[DBEnv, Throwable, List[JobRun]] = ZIO.accessM(_.get.getJobRuns(args))
  def getJobLogs(args: JobLogsArgs): ZIO[DBEnv, Throwable, List[JobLogs]] = ZIO.accessM(_.get.getJobLogs(args))
  def getCredentials: ZIO[DBEnv, Throwable, List[GetCredential]] = ZIO.accessM(_.get.getCredentials)
  def updateSuccessJob(job: String, ts: Long): ZIO[DBEnv, Throwable, Long] = ZIO.accessM(_.get.updateSuccessJob(job,ts))
  def updateFailedJob(job: String, ts: Long): ZIO[DBEnv, Throwable, Long] = ZIO.accessM(_.get.updateFailedJob(job, ts))
  def updateJobState(args: EtlJobStateArgs): ZIO[DBEnv, Throwable, Boolean] = ZIO.accessM(_.get.updateJobState(args))
  def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): ZIO[DBEnv, Throwable, Credentials] = ZIO.accessM(_.get.addCredential(credentialsDB,actualSerializerOutput))
  def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): ZIO[DBEnv, Throwable, Credentials] = ZIO.accessM(_.get.updateCredential(credentialsDB,actualSerializerOutput))
  def refreshJobs(jobs: List[EtlJob]): ZIO[DBEnv, Throwable, List[JobDB]] = ZIO.accessM(_.get.refreshJobs(jobs))
  def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.updateStepRun(job_run_id, step_name, props, status, elapsed_time))
  def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time))
  def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time))
  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.updateJobRun(job_run_id, status, elapsed_time))
  def executeQueryWithResponse[T <: Product : Read](query: String): ZIO[DBEnv, Throwable, List[T]] = ZIO.accessM(_.get.executeQueryWithResponse(query))
  def executeQuery(query: String): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.executeQuery(query))
  def executeQueryWithSingleResponse[T : Read](query: String):ZIO[DBEnv, Throwable, T] = ZIO.accessM(_.get.executeQueryWithSingleResponse(query))
  def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T):ZIO[DBEnv, Throwable, T] = ZIO.accessM(_.get.executeQuerySingleOutput(query)(fn))
}
