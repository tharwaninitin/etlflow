package etlflow.webserver.api

import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

trait GqlService {
  def runJob(args: EtlJobArgs): ZIO[EtlFlowHas with Blocking with Clock, Throwable, EtlJob]
  def updateJobState(args: EtlJobStateArgs): ZIO[EtlFlowHas, Throwable, Boolean]
  def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth]
  def addCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob]
  def updateCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob]
  def addCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials]
  def updateCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials]
  def getCurrentTime: ZIO[EtlFlowHas, Throwable, CurrentTime]
  def getQueueStats: ZIO[EtlFlowHas, Throwable, List[QueueDetails]]
  def getJobLogs(args: JobLogsArgs): ZIO[EtlFlowHas, Throwable, List[JobLogs]]
  def getCredentials: ZIO[EtlFlowHas, Throwable, List[UpdateCredentialDB]]

  def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics]
  def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]]
  def getCacheStats: ZIO[EtlFlowHas, Throwable, List[CacheDetails]]
  def getDbJobRuns(args: DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]]
  def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]]

  def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus]
}

object GqlService {

  def runJob(args: EtlJobArgs): ZIO[EtlFlowHas with Blocking with Clock, Throwable, EtlJob] =
    ZIO.accessM[EtlFlowHas with Blocking with Clock](_.get.runJob(args))

  def updateJobState(args: EtlJobStateArgs): ZIO[EtlFlowHas, Throwable, Boolean] =
    ZIO.accessM[EtlFlowHas](_.get.updateJobState(args))

  def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics] =
    ZIO.accessM[EtlFlowHas](_.get.getInfo)

  def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus] =
    ZStream.accessStream[EtlFlowHas](_.get.notifications)

  def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth] =
    ZIO.accessM[EtlFlowHas](_.get.login(args))

  def addCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob] =
    ZIO.accessM[EtlFlowHas](_.get.addCronJob(args))

  def updateCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob] =
    ZIO.accessM[EtlFlowHas](_.get.updateCronJob(args))

  def getDbJobRuns(args: DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]] =
    ZIO.accessM[EtlFlowHas](_.get.getDbJobRuns(args))

  def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]] =
    ZIO.accessM[EtlFlowHas](_.get.getDbStepRuns(args))

  def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] =
    ZIO.accessM[EtlFlowHas](_.get.getJobs)

  def getCacheStats: ZIO[EtlFlowHas, Throwable, List[CacheDetails]] =
    ZIO.accessM[EtlFlowHas](_.get.getCacheStats)

  def addCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials] =
    ZIO.accessM[EtlFlowHas](_.get.addCredentials(args))

  def updateCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials] =
    ZIO.accessM[EtlFlowHas](_.get.updateCredentials(args))

  def getCurrentTime: ZIO[EtlFlowHas, Throwable, CurrentTime] =
    ZIO.accessM[EtlFlowHas](_.get.getCurrentTime)

  def getQueueStats: ZIO[EtlFlowHas, Throwable, List[QueueDetails]] =
    ZIO.accessM[EtlFlowHas](_.get.getQueueStats)

  def getJobLogs(args: JobLogsArgs): ZIO[EtlFlowHas, Throwable, List[JobLogs]] =
    ZIO.accessM[EtlFlowHas](_.get.getJobLogs(args))

  def getCredentials: ZIO[EtlFlowHas, Throwable, List[UpdateCredentialDB]] =
    ZIO.accessM[EtlFlowHas](_.get.getCredentials)

}
