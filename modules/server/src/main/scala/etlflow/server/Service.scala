package etlflow.server

import etlflow.db._
import etlflow.server.model._
import etlflow.json.JsonEnv
import zio.{RIO, ZEnv, ZIO}

private[etlflow] trait Service {
  def runJob(args: EtlJobArgs, submitter: String): RIO[ServerEnv, EtlJob]
  def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean]
  def login(args: UserArgs): ZIO[APIEnv with DBServerEnv, Throwable, UserAuth]
  def addCredentials(args: CredentialsArgs): RIO[ServerEnv, Credential]
  def updateCredentials(args: CredentialsArgs): RIO[ServerEnv, Credential]
  def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime]
  def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]]
  def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]]
  def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics]
  def getJobs: ZIO[ServerEnv, Throwable, List[Job]]
  def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]]
  def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]]
}

private[etlflow] object Service {

  def runJob(args: EtlJobArgs, submitter: String): ZIO[ServerEnv, Throwable, EtlJob] =
    ZIO.accessM[APIEnv with ZEnv with JsonEnv with DBServerEnv](_.get.runJob(args, submitter)).absorb

  def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.updateJobState(args))

  def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics] =
    ZIO.accessM[APIEnv](_.get.getInfo)

  def login(args: UserArgs): ZIO[APIEnv with DBServerEnv, Throwable, UserAuth] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.login(args))

  def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getDbJobRuns(args))

  def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getDbStepRuns(args))

  def getJobs: ZIO[ServerEnv, Throwable, List[Job]] =
    ZIO.accessM[APIEnv with ZEnv with JsonEnv with DBServerEnv](_.get.getJobs)

  def addCredentials(args: CredentialsArgs): RIO[ServerEnv, Credential] =
    ZIO.accessM[APIEnv with ZEnv with JsonEnv with DBServerEnv](_.get.addCredentials(args))

  def updateCredentials(args: CredentialsArgs): RIO[ServerEnv, Credential] =
    ZIO.accessM[APIEnv with ZEnv with JsonEnv with DBServerEnv](_.get.updateCredentials(args))

  def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime] =
    ZIO.accessM[APIEnv](_.get.getCurrentTime)

  def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getJobLogs(args))

  def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getCredentials)
}
