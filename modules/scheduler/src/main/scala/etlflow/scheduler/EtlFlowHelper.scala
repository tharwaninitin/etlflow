package etlflow.scheduler

import zio.blocking.Blocking
import zio.stream.ZStream
import zio.{Has, ZIO}

object EtlFlowHelper {

  case class EtlJobNameArgs(name: String)
  case class Props(key: String, value: String)
  case class EtlJobArgs(name: String, props: List[Props])
  case class EtlJob(name: String, props: Map[String,String])
  case class EtlJobStatus(name: String, status: String, props: Map[String,String])
  case class EtlFlowInfo(active_jobs: Int, active_subscribers: Int)

  object EtlFlow {
    trait Service {
      def getEtlJobs(args: EtlJobNameArgs): ZIO[EtlFlowHas, Throwable, List[EtlJob]]
      def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob]
      def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowInfo]
      def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus]
      // def getStream: ZStream[EtlFlowHas, Throwable, EtlFlowInfo]
      // def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowInfo]
    }
  }

  type EtlFlowHas = Has[EtlFlow.Service]

  def getEtlJobs(args: EtlJobNameArgs): ZIO[EtlFlowHas, Throwable, List[EtlJob]] =
    ZIO.accessM[EtlFlowHas](_.get.getEtlJobs(args))

  def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob] =
    ZIO.accessM[EtlFlowHas](_.get.runJob(args))

  def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowInfo] =
    ZIO.accessM[EtlFlowHas](_.get.getInfo)

  def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus] =
    ZStream.accessStream[EtlFlowHas](_.get.notifications)

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
