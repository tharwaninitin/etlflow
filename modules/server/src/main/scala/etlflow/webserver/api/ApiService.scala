package etlflow.webserver.api

import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

trait ApiService {
  def runJob(args: EtlJobArgs, submitter: String): ZIO[GQLEnv with Blocking with Clock, Throwable, EtlJob]
  def updateJobState(args: EtlJobStateArgs): ZIO[GQLEnv, Throwable, Boolean]
  def login(args: UserArgs): ZIO[GQLEnv, Throwable, UserAuth]
  def addCronJob(args: CronJobArgs): ZIO[GQLEnv, Throwable, CronJob]
  def updateCronJob(args: CronJobArgs): ZIO[GQLEnv, Throwable, CronJob]
  def addCredentials(args: CredentialsArgs): ZIO[GQLEnv, Throwable, Credentials]
  def updateCredentials(args: CredentialsArgs): ZIO[GQLEnv, Throwable, Credentials]
  def getCurrentTime: ZIO[GQLEnv, Throwable, CurrentTime]
  def getQueueStats: ZIO[GQLEnv, Throwable, List[QueueDetails]]
  def getJobLogs(args: JobLogsArgs): ZIO[GQLEnv, Throwable, List[JobLogs]]
  def getCredentials: ZIO[GQLEnv, Throwable, List[UpdateCredentialDB]]
  def getInfo: ZIO[GQLEnv, Throwable, EtlFlowMetrics]
  def getJobs: ZIO[GQLEnv, Throwable, List[Job]]
  def getCacheStats: ZIO[GQLEnv, Throwable, List[CacheDetails]]
  def getDbJobRuns(args: DbJobRunArgs): ZIO[GQLEnv, Throwable, List[JobRun]]
  def getDbStepRuns(args: DbStepRunArgs): ZIO[GQLEnv, Throwable, List[StepRun]]
  def notifications: ZStream[GQLEnv, Nothing, EtlJobStatus]
}

object ApiService {

  def runJob(args: EtlJobArgs, submitter: String): ZIO[GQLEnv with Blocking with Clock, Throwable, EtlJob] =
    ZIO.accessM[GQLEnv with Blocking with Clock](_.get.runJob(args,submitter)).absorb

  def updateJobState(args: EtlJobStateArgs): ZIO[GQLEnv, Throwable, Boolean] =
    ZIO.accessM[GQLEnv](_.get.updateJobState(args))

  def getInfo: ZIO[GQLEnv, Throwable, EtlFlowMetrics] =
    ZIO.accessM[GQLEnv](_.get.getInfo)

  def notifications: ZStream[GQLEnv, Nothing, EtlJobStatus] =
    ZStream.accessStream[GQLEnv](_.get.notifications)

  def login(args: UserArgs): ZIO[GQLEnv, Throwable, UserAuth] =
    ZIO.accessM[GQLEnv](_.get.login(args))

  def addCronJob(args: CronJobArgs): ZIO[GQLEnv, Throwable, CronJob] =
    ZIO.accessM[GQLEnv](_.get.addCronJob(args))

  def updateCronJob(args: CronJobArgs): ZIO[GQLEnv, Throwable, CronJob] =
    ZIO.accessM[GQLEnv](_.get.updateCronJob(args))

  def getDbJobRuns(args: DbJobRunArgs): ZIO[GQLEnv, Throwable, List[JobRun]] =
    ZIO.accessM[GQLEnv](_.get.getDbJobRuns(args))

  def getDbStepRuns(args: DbStepRunArgs): ZIO[GQLEnv, Throwable, List[StepRun]] =
    ZIO.accessM[GQLEnv](_.get.getDbStepRuns(args))

  def getJobs: ZIO[GQLEnv, Throwable, List[Job]] =
    ZIO.accessM[GQLEnv](_.get.getJobs)

  def getCacheStats: ZIO[GQLEnv, Throwable, List[CacheDetails]] =
    ZIO.accessM[GQLEnv](_.get.getCacheStats)

  def addCredentials(args: CredentialsArgs): ZIO[GQLEnv, Throwable, Credentials] =
    ZIO.accessM[GQLEnv](_.get.addCredentials(args))

  def updateCredentials(args: CredentialsArgs): ZIO[GQLEnv, Throwable, Credentials] =
    ZIO.accessM[GQLEnv](_.get.updateCredentials(args))

  def getCurrentTime: ZIO[GQLEnv, Throwable, CurrentTime] =
    ZIO.accessM[GQLEnv](_.get.getCurrentTime)

  def getQueueStats: ZIO[GQLEnv, Throwable, List[QueueDetails]] =
    ZIO.accessM[GQLEnv](_.get.getQueueStats)

  def getJobLogs(args: JobLogsArgs): ZIO[GQLEnv, Throwable, List[JobLogs]] =
    ZIO.accessM[GQLEnv](_.get.getJobLogs(args))

  def getCredentials: ZIO[GQLEnv, Throwable, List[UpdateCredentialDB]] =
    ZIO.accessM[GQLEnv](_.get.getCredentials)

}
