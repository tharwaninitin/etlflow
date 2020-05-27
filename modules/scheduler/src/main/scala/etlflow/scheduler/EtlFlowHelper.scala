package etlflow.scheduler

import cron4s.CronExpr
import zio.stream.ZStream
import zio.{Has, ZIO}
object EtlFlowHelper {

  // GraphQL ARGS and Results
  case class Props(key: String, value: String)
  case class EtlJobArgs(name: String, props: List[Props])
  case class UserArgs(user_name: String, password: String)

  case class EtlJob(name: String, props: Map[String,String])
  case class EtlJobStatus(name: String, status: String, props: Map[String,String])
  case class EtlFlowMetrics(
                             active_jobs: Int,
                             active_subscribers: Int,
                             etl_jobs: Int,
                             cron_jobs: Int
                           )
  case class UserAuth(message: String, token: String)
  case class CronJob(job_name: String, schedule: CronExpr)

  object EtlFlow {
    trait Service {
      def getEtlJobs: ZIO[EtlFlowHas, Throwable, List[EtlJob]]
      def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob]
      def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics]
      def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus]
      def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth]
      def addCronJob(args: CronJob): ZIO[EtlFlowHas, Throwable, CronJob]
      def updateCronJob(args: CronJob): ZIO[EtlFlowHas, Throwable, CronJob]
      def getCronJobs: ZIO[EtlFlowHas, Throwable, List[CronJob]]
      // def getStream: ZStream[EtlFlowHas, Throwable, EtlFlowInfo]
      // def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowInfo]
    }
  }

  type EtlFlowHas = Has[EtlFlow.Service]

  def getEtlJobs: ZIO[EtlFlowHas, Throwable, List[EtlJob]] =
    ZIO.accessM[EtlFlowHas](_.get.getEtlJobs)

  def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob] =
    ZIO.accessM[EtlFlowHas](_.get.runJob(args))

  def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics] =
    ZIO.accessM[EtlFlowHas](_.get.getInfo)

  def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus] =
    ZStream.accessStream[EtlFlowHas](_.get.notifications)

  def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth] =
    ZIO.accessM[EtlFlowHas](_.get.login(args))

  def addCronJob(args: CronJob): ZIO[EtlFlowHas, Throwable, CronJob] =
    ZIO.accessM[EtlFlowHas](_.get.addCronJob(args))

  def updateCronJob(args: CronJob): ZIO[EtlFlowHas, Throwable, CronJob] =
    ZIO.accessM[EtlFlowHas](_.get.updateCronJob(args))

  def getCronJobs: ZIO[EtlFlowHas, Throwable, List[CronJob]] =
    ZIO.accessM[EtlFlowHas](_.get.getCronJobs)

  // def getStream: ZStream[EtlFlowHas, Throwable, EtlFlowInfo] =
  //  ZStream.accessStream[EtlFlowHas](_.get.getStream)
  //
  // def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowInfo] = {
  //   val x = ZIO.accessM[Blocking](_.get.blocking{
  //     ZIO.unit
  //   })
  //   ZIO.accessM[EtlFlowHas](_.get.getLogs)
  // }

}
