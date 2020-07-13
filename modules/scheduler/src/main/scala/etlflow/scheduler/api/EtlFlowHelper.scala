package etlflow.scheduler.api

import cron4s.CronExpr
import etlflow.log.JobRun
import zio.stream.ZStream
import zio.{Has, ZIO}

object EtlFlowHelper {

  // GraphQL ARGS and Results
  case class Props(key: String, value: String)
  case class EtlJobStateArgs(name: String, state: Boolean)
  case class EtlJobArgs(name: String, props: List[Props])
  case class UserArgs(user_name: String, password: String)
  case class DbJobRunArgs(jobRunId: Option[String] = None, jobName: Option[String] = None, limit: Int, offset: Int)
  case class CronJobArgs(job_name: String, schedule: CronExpr)

  case class EtlJob(name: String, props: Map[String,String])
  case class EtlJobStatus(name: String, status: String, props: Map[String,String])
  case class EtlFlowMetrics(
           active_jobs: Int,
           active_subscribers: Int,
           etl_jobs: Int,
           cron_jobs: Int,
           used_memory: String,
           free_memory: String,
           total_memory: String,
           max_memory: String,
         )
  case class UserAuth(message: String, token: String)
  case class CronJob(job_name: String, schedule: Option[CronExpr], failed: Long, success: Long)
  case class Job(name: String, props: Map[String,String], schedule: Option[CronExpr], failed: Long, success: Long)

  object EtlFlow {
    trait Service {
      def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob]
      def updateJobState(args: EtlJobStateArgs): ZIO[EtlFlowHas, Throwable, Boolean]
      def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth]
      def addCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob]
      def updateCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob]

      def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics]
      def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]]
      def getDbJobRuns(args: DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]]

      def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus]
      def getStream: ZStream[EtlFlowHas, Nothing, EtlFlowMetrics]
      // def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowInfo]
    }
  }

  type EtlFlowHas = Has[EtlFlow.Service]

  def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob] =
    ZIO.accessM[EtlFlowHas](_.get.runJob(args))

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

  def getStream: ZStream[EtlFlowHas, Nothing, EtlFlowMetrics] =
    ZStream.accessStream[EtlFlowHas](_.get.getStream)

  def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] =
    ZIO.accessM[EtlFlowHas](_.get.getJobs)
  //
  // def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowInfo] = {
  //   val x = ZIO.accessM[Blocking](_.get.blocking{
  //     ZIO.unit
  //   })
  //   ZIO.accessM[EtlFlowHas](_.get.getLogs)
  // }

}
