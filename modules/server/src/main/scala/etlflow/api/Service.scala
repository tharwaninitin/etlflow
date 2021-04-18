package etlflow.api

import etlflow.TransactorEnv
import etlflow.jdbc.DBEnv
import etlflow.log.{JobRun, StepRun}
import etlflow.api.Schema._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

trait Service {
  def runJob(args: EtlJobArgs, submitter: String): ZIO[GQLEnv with DBEnv with TransactorEnv with Blocking with Clock, Throwable, EtlJob]
  def updateJobState(args: EtlJobStateArgs): ZIO[GQLEnv with DBEnv, Throwable, Boolean]
  def login(args: UserArgs): ZIO[GQLEnv with DBEnv, Throwable, UserAuth]
  def addCredentials(args: CredentialsArgs): ZIO[GQLEnv with DBEnv, Throwable, Credentials]
  def updateCredentials(args: CredentialsArgs): ZIO[GQLEnv with DBEnv, Throwable, Credentials]
  def getCurrentTime: ZIO[GQLEnv, Throwable, CurrentTime]
  def getQueueStats: ZIO[GQLEnv, Throwable, List[QueueDetails]]
  def getJobLogs(args: JobLogsArgs): ZIO[GQLEnv with DBEnv, Throwable, List[JobLogs]]
  def getCredentials: ZIO[GQLEnv with DBEnv, Throwable, List[GetCredential]]
  def getInfo: ZIO[GQLEnv, Throwable, EtlFlowMetrics]
  def getJobs: ZIO[GQLEnv with DBEnv, Throwable, List[Job]]
  def getCacheStats: ZIO[GQLEnv, Throwable, List[CacheDetails]]
  def getDbJobRuns(args: DbJobRunArgs): ZIO[GQLEnv with DBEnv, Throwable, List[JobRun]]
  def getDbStepRuns(args: DbStepRunArgs): ZIO[GQLEnv with DBEnv, Throwable, List[StepRun]]
  def notifications: ZStream[GQLEnv, Nothing, EtlJobStatus]
}

object Service {

  def runJob(args: EtlJobArgs, submitter: String): ZIO[GQLEnv with DBEnv with TransactorEnv with Blocking with Clock, Throwable, EtlJob] =
    ZIO.accessM[GQLEnv with DBEnv with TransactorEnv with Blocking with Clock](_.get.runJob(args,submitter)).absorb

  def updateJobState(args: EtlJobStateArgs): ZIO[GQLEnv with DBEnv, Throwable, Boolean] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.updateJobState(args))

  def getInfo: ZIO[GQLEnv, Throwable, EtlFlowMetrics] =
    ZIO.accessM[GQLEnv](_.get.getInfo)

  def notifications: ZStream[GQLEnv, Nothing, EtlJobStatus] =
    ZStream.accessStream[GQLEnv](_.get.notifications)

  def login(args: UserArgs): ZIO[GQLEnv with DBEnv, Throwable, UserAuth] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.login(args))

  def getDbJobRuns(args: DbJobRunArgs): ZIO[GQLEnv with DBEnv, Throwable, List[JobRun]] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.getDbJobRuns(args))

  def getDbStepRuns(args: DbStepRunArgs): ZIO[GQLEnv with DBEnv, Throwable, List[StepRun]] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.getDbStepRuns(args))

  def getJobs: ZIO[GQLEnv with DBEnv, Throwable, List[Job]] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.getJobs)

  def getCacheStats: ZIO[GQLEnv, Throwable, List[CacheDetails]] =
    ZIO.accessM[GQLEnv](_.get.getCacheStats)

  def addCredentials(args: CredentialsArgs): ZIO[GQLEnv with DBEnv, Throwable, Credentials] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.addCredentials(args))

  def updateCredentials(args: CredentialsArgs): ZIO[GQLEnv with DBEnv, Throwable, Credentials] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.updateCredentials(args))

  def getCurrentTime: ZIO[GQLEnv, Throwable, CurrentTime] =
    ZIO.accessM[GQLEnv](_.get.getCurrentTime)

  def getQueueStats: ZIO[GQLEnv, Throwable, List[QueueDetails]] =
    ZIO.accessM[GQLEnv](_.get.getQueueStats)

  def getJobLogs(args: JobLogsArgs): ZIO[GQLEnv with DBEnv, Throwable, List[JobLogs]] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.getJobLogs(args))

  def getCredentials: ZIO[GQLEnv with DBEnv, Throwable, List[GetCredential]] =
    ZIO.accessM[GQLEnv with DBEnv](_.get.getCredentials)

}
