package etlflow.api

import etlflow.DBEnv
import etlflow.jdbc._
import etlflow.api.Schema._
import etlflow.schema._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

trait Service {
  def runJob(args: EtlJobArgs, submitter: String): ZIO[APIEnv with DBServerEnv with DBEnv with Blocking with Clock, Throwable, EtlJob]
  def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean]
  def login(args: UserArgs): ZIO[APIEnv with DBServerEnv, Throwable, UserAuth]
  def addCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials]
  def updateCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials]
  def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime]
  def getQueueStats: ZIO[APIEnv, Throwable, List[QueueDetails]]
  def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]]
  def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]]
  def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics]
  def getJobs: ZIO[APIEnv with DBServerEnv, Throwable, List[Job]]
  def getCacheStats: ZIO[APIEnv, Throwable, List[CacheDetails]]
  def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]]
  def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]]
  def getJobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]]
}

object Service {

  def runJob(args: EtlJobArgs, submitter: String): ZIO[APIEnv with DBServerEnv with DBEnv with Blocking with Clock, Throwable, EtlJob] =
    ZIO.accessM[APIEnv with DBServerEnv with DBEnv with Blocking with Clock](_.get.runJob(args,submitter)).absorb

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

  def getJobs: ZIO[APIEnv with DBServerEnv, Throwable, List[Job]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getJobs)

  def getCacheStats: ZIO[APIEnv, Throwable, List[CacheDetails]] =
    ZIO.accessM[APIEnv](_.get.getCacheStats)

  def addCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.addCredentials(args))

  def updateCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.updateCredentials(args))

  def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime] =
    ZIO.accessM[APIEnv](_.get.getCurrentTime)

  def getQueueStats: ZIO[APIEnv, Throwable, List[QueueDetails]] =
    ZIO.accessM[APIEnv](_.get.getQueueStats)

  def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getJobLogs(args))

  def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]] =
    ZIO.accessM[APIEnv with DBServerEnv](_.get.getCredentials)

  def getJobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]] =
    ZIO.accessM[APIEnv](_.get.getJobStats)
}
