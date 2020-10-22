package etlflow.utils

import cron4s.CronExpr
import etlflow.log.{JobRun, StepRun}
import zio.stream.ZStream
import zio.{Has, RIO, ZEnv, ZIO}

object EtlFlowHelper {

  type EtlFlowTask[A] = RIO[ZEnv with EtlFlowHas, A]

  // DB Objects
  case class UserInfo(user_name: String, password: String, user_active: String)
  case class CronJobDB(job_name: String, schedule: String, failed: Long, success: Long, is_active: Boolean)
  case class CredentialDB(name: String, `type`: String, value: String)

  // GraphQL ARGS and Results
  sealed trait Creds
  object Creds {
    case object AWS extends Creds
    case object JDBC extends Creds
  }
  case class Props(key: String, value: String)
  case class EtlJobStateArgs(name: String, state: Boolean)
  case class EtlJobArgs(name: String, props: List[Props])
  case class UserArgs(user_name: String, password: String)
  case class DbJobRunArgs(
                           jobRunId: Option[String] = None,
                           jobName: Option[String] = None,
                           startTime: Option[java.time.LocalDate] = None,
                           endTime: Option[java.time.LocalDate] = None,
                           filter: Option[String] = None,
                           limit: Int, offset: Int
                         )
  case class DbStepRunArgs(job_run_id: String)
  case class CronJobArgs(job_name: String, schedule: CronExpr)
  case class CredentialsArgs(name: String, `type`: Option[Creds], value: List[Props])

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
                             current_time: String,
                             build_time: String
                           )
  case class CurrentTime(current_time:String)
  case class UserAuth(message: String, token: String)
  case class CronJob(job_name: String, schedule: Option[CronExpr], failed: Long, success: Long)
  case class Credentials(name: String, `type`: String, value: String)
  case class CacheInfo(name:String,hitCount:Long,hitRate:Double,size:Long,missCount:Long,missRate:Double,requestCount:Long,data: Map[String,String])

  case class Job(name: String, props: Map[String,String], schedule: Option[CronExpr],nextSchedule: String,schduleRemainingTime: String ,failed: Long, success: Long, is_active:Boolean,max_active_runs: Int, job_deploy_mode: String)

  object EtlFlow {
    trait Service {
      def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob]
      def updateJobState(args: EtlJobStateArgs): ZIO[EtlFlowHas, Throwable, Boolean]
      def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth]
      def addCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob]
      def updateCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob]
      def addCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials]
      def updateCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials]
      def getCurrentTime: ZIO[EtlFlowHas, Throwable, CurrentTime]

      def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics]
      def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]]
      def getCacheStats: ZIO[EtlFlowHas, Throwable, List[CacheInfo]]
      def getDbJobRuns(args: DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]]
      def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]]

      def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus]
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

  def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]] =
    ZIO.accessM[EtlFlowHas](_.get.getDbStepRuns(args))

  def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] =
    ZIO.accessM[EtlFlowHas](_.get.getJobs)

  def getCacheStats: ZIO[EtlFlowHas, Throwable, List[CacheInfo]] =
    ZIO.accessM[EtlFlowHas](_.get.getCacheStats)

  def addCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials] =
    ZIO.accessM[EtlFlowHas](_.get.addCredentials(args))

  def updateCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials] =
    ZIO.accessM[EtlFlowHas](_.get.updateCredentials(args))

  def getCurrentTime: ZIO[EtlFlowHas, Throwable, CurrentTime] =
    ZIO.accessM[EtlFlowHas](_.get.getCurrentTime)

}