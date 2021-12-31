package etlflow.server

import etlflow.db._
import zio.{IO, ZIO}

private[etlflow] object DBServerApi {

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

    def addCredential(cred: Credential): IO[Throwable, Credential]

    def updateCredential(cred: Credential): IO[Throwable, Credential]

    def refreshJobs(jobs: List[EtlJob]): IO[Throwable, List[JobDB]]
  }

  def getUser(user_name: String): ZIO[DBServerEnv, Throwable, UserDB] = ZIO.accessM(_.get.getUser(user_name))

  def getJob(name: String): ZIO[DBServerEnv, Throwable, JobDB] = ZIO.accessM(_.get.getJob(name))

  def getJobs: ZIO[DBServerEnv, Throwable, List[JobDBAll]] = ZIO.accessM(_.get.getJobs)

  def getStepRuns(args: DbStepRunArgs): ZIO[DBServerEnv, Throwable, List[StepRun]] = ZIO.accessM(_.get.getStepRuns(args))

  def getJobRuns(args: DbJobRunArgs): ZIO[DBServerEnv, Throwable, List[JobRun]] = ZIO.accessM(_.get.getJobRuns(args))

  def getJobLogs(args: JobLogsArgs): ZIO[DBServerEnv, Throwable, List[JobLogs]] = ZIO.accessM(_.get.getJobLogs(args))

  def getCredentials: ZIO[DBServerEnv, Throwable, List[GetCredential]] = ZIO.accessM(_.get.getCredentials)

  def updateSuccessJob(job: String, ts: Long): ZIO[DBServerEnv, Throwable, Long] = ZIO.accessM(_.get.updateSuccessJob(job, ts))

  def updateFailedJob(job: String, ts: Long): ZIO[DBServerEnv, Throwable, Long] = ZIO.accessM(_.get.updateFailedJob(job, ts))

  def updateJobState(args: EtlJobStateArgs): ZIO[DBServerEnv, Throwable, Boolean] = ZIO.accessM(_.get.updateJobState(args))

  def addCredential(cred: Credential): ZIO[DBServerEnv, Throwable, Credential] = ZIO.accessM(_.get.addCredential(cred))

  def updateCredential(cred: Credential): ZIO[DBServerEnv, Throwable, Credential] = ZIO.accessM(_.get.updateCredential(cred))

  def refreshJobs(jobs: List[EtlJob]): ZIO[DBServerEnv, Throwable, List[JobDB]] = ZIO.accessM(_.get.refreshJobs(jobs))
}
