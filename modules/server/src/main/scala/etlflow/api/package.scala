package etlflow

import cron4s.CronExpr
import zio.{Has, RIO}

package object api {

  private[etlflow] type APIEnv = Has[Service]
  private[etlflow] type ServerEnv = APIEnv with JobEnv
  private[etlflow] type ServerTask[A] = RIO[ServerEnv, A]

  private[etlflow] object Schema {

    // case class Props(key: String, value: String)
    case class EtlJobArgs(name: String, props: Option[List[Props]] = None)
    case class CronJobArgs(job_name: String, schedule: CronExpr)
    case class EtlFlowMetrics(active_jobs: Int, active_subscribers: Int, etl_jobs: Int, cron_jobs: Int, build_time: String)
    case class CurrentTime(current_time:String)
    case class UserAuth(message: String, token: String)
    case class CacheInfo(name:String,hitCount:Long,hitRate:Double,size:Long,missCount:Long,missRate:Double,requestCount:Long,data: Map[String,String])
    case class CacheDetails(name:String,details:Map[String,String])
    case class QueueDetails(name:String,details:String,submitted_from:String,execution_time:String)
    case class EtlJobStatus(id: String, name: String, start_time: String, `type`: String, status: String)
    case class Props(key: String, value: String)
    case class CredentialsArgs(name: String, `type`: Creds, value: List[Props])
    sealed trait Creds
    object Creds {
      case object AWS extends Creds
      case object JDBC extends Creds
    }
    case class UserArgs(user_name: String, password: String)
  }
}
