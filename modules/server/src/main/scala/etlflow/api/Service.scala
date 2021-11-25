package etlflow.api

import etlflow.api.Schema._
import etlflow.cache.{CacheDetails, CacheEnv}
import etlflow.core.CoreEnv
import etlflow.crypto.CryptoEnv
import etlflow.db._
import etlflow.json.JsonEnv
import zio.{RIO, ZIO}

private[etlflow] trait Service {
  def runJob(args: EtlJobArgs, submitter: String): RIO[ServerEnv, EtlJob]
  def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean]
  def login(args: UserArgs): ZIO[APIEnv with DBServerEnv with CacheEnv, Throwable, UserAuth]
  def addCredentials(args: CredentialsArgs): RIO[ServerEnv, Credentials]
  def updateCredentials(args: CredentialsArgs): RIO[ServerEnv, Credentials]
  def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime]
  def getQueueStats: ZIO[APIEnv with CacheEnv, Throwable, List[QueueDetails]]
  def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]]
  def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]]
  def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics]
  def getJobs: ZIO[ServerEnv, Throwable, List[Job]]
  def getCacheStats: ZIO[APIEnv with CacheEnv with JsonEnv, Throwable, List[CacheDetails]]
  def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]]
  def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]]
  def getJobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]]
}

private[etlflow] object Service {

  def runJob(args: EtlJobArgs, submitter: String): ZIO[ServerEnv, Throwable, EtlJob] =
    ZIO.accessM[APIEnv with CoreEnv with CacheEnv with CryptoEnv with DBServerEnv](_.get.runJob(args,submitter)).absorb

  def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.updateJobState(args))

  def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics] =
    ZIO.accessM[APIEnv](_.get.getInfo)

  def login(args: UserArgs): ZIO[APIEnv with DBServerEnv with CacheEnv, Throwable, UserAuth] =
    ZIO.accessM[APIEnv with DBServerEnv with CacheEnv](_.get.login(args))

  def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getDbJobRuns(args))

  def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getDbStepRuns(args))

  def getJobs: ZIO[ServerEnv, Throwable, List[Job]] =
    ZIO.accessM[APIEnv with CoreEnv with CacheEnv with CryptoEnv with DBServerEnv](_.get.getJobs)

  def getCacheStats: ZIO[APIEnv with CacheEnv with JsonEnv, Throwable, List[CacheDetails]] =
    ZIO.accessM[APIEnv with CacheEnv with JsonEnv](_.get.getCacheStats)

  def addCredentials(args: CredentialsArgs): RIO[ServerEnv, Credentials] =
    ZIO.accessM[APIEnv with CoreEnv with CacheEnv with CryptoEnv with DBServerEnv](_.get.addCredentials(args))

  def updateCredentials(args: CredentialsArgs): RIO[ServerEnv, Credentials] =
    ZIO.accessM[APIEnv with CoreEnv with CacheEnv with CryptoEnv with DBServerEnv](_.get.updateCredentials(args))

  def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime] =
    ZIO.accessM[APIEnv](_.get.getCurrentTime)

  def getQueueStats: ZIO[APIEnv with CacheEnv, Throwable, List[QueueDetails]] =
    ZIO.accessM[APIEnv with CacheEnv](_.get.getQueueStats)

  def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getJobLogs(args))

  def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getCredentials)

  def getJobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]] =
    ZIO.accessM[APIEnv](_.get.getJobStats)
}
