package etlflow.server

import etlflow.db._
import etlflow.server.model._
import etlflow.json.JsonEnv
import zio._

private[etlflow] trait Service {
  def getCurrentTime: UIO[String]
  def getMetrics: UIO[EtlFlowMetrics]
  def runJob(name: String, props: Map[String, String], submitter: String): RIO[ZEnv with JsonEnv with DBServerEnv, EtlJob]
  def getJobs: RIO[DBServerEnv, List[Job]]
  def login(args: UserArgs): RIO[DBServerEnv, UserAuth]
  def getJobLogs(args: JobLogsArgs): RIO[DBServerEnv, List[JobLogs]]
  def getJobRuns(args: DbJobRunArgs): RIO[DBServerEnv, List[JobRun]]
  def getStepRuns(args: DbStepRunArgs): RIO[DBServerEnv, List[StepRun]]
  def updateJobState(args: EtlJobStateArgs): RIO[DBServerEnv, Boolean]
  def getCredentials: RIO[DBServerEnv, List[GetCredential]]
  def addCredential(args: CredentialsArgs): RIO[JsonEnv with DBServerEnv, Credential]
  def updateCredential(args: CredentialsArgs): RIO[JsonEnv with DBServerEnv, Credential]
}

private[etlflow] object Service {

  def getCurrentTime: RIO[APIEnv, String] = ZIO.accessM[APIEnv](_.get.getCurrentTime)

  def getMetrics: RIO[APIEnv, EtlFlowMetrics] = ZIO.accessM[APIEnv](_.get.getMetrics)

  def runJob(name: String, props: Map[String, String], submitter: String): RIO[ServerEnv, EtlJob] =
    ZIO.accessM[APIEnv with ZEnv with JsonEnv with DBServerEnv](_.get.runJob(name, props, submitter)).absorb

  def getJobs: RIO[APIEnv with DBServerEnv, List[Job]] = ZIO.accessM[APIEnv with DBServerEnv](_.get.getJobs)

  def login(args: UserArgs): RIO[APIEnv with DBServerEnv, UserAuth] = ZIO.accessM[APIEnv with DBServerEnv](_.get.login(args))

  def getJobRuns(args: DbJobRunArgs): RIO[APIEnv with DBServerEnv, List[JobRun]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getJobRuns(args))

  def getStepRuns(args: DbStepRunArgs): RIO[APIEnv with DBServerEnv, List[StepRun]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getStepRuns(args))

  def getJobLogs(args: JobLogsArgs): RIO[APIEnv with DBServerEnv, List[JobLogs]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getJobLogs(args))

  def updateJobState(args: EtlJobStateArgs): RIO[APIEnv with DBServerEnv, Boolean] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.updateJobState(args))

  def getCredentials: RIO[APIEnv with DBServerEnv, List[GetCredential]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getCredentials)

  def addCredential(args: CredentialsArgs): RIO[APIEnv with JsonEnv with DBServerEnv, Credential] =
    ZIO.accessM[APIEnv with JsonEnv with DBServerEnv](_.get.addCredential(args))

  def updateCredential(args: CredentialsArgs): RIO[APIEnv with JsonEnv with DBServerEnv, Credential] =
    ZIO.accessM[APIEnv with JsonEnv with DBServerEnv](_.get.updateCredential(args))
}
