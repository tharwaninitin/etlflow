package etlflow

import etlflow.cache.CacheEnv
import etlflow.crypto.CryptoEnv
import etlflow.db.DBServerEnv
import zio.{Has, RIO}

package object api {

  private[etlflow] type APIEnv = Has[Service]
  private[etlflow] type ServerEnv = APIEnv with JobEnv with CacheEnv with CryptoEnv with DBServerEnv
  private[etlflow] type ServerTask[A] = RIO[ServerEnv, A]

  private[etlflow] object Schema {

    // case class Props(key: String, value: String)
    case class EtlJobArgs(name: String, props: Option[List[Props]] = None)
    case class EtlFlowMetrics(active_jobs: Int, active_subscribers: Int, etl_jobs: Int, cron_jobs: Int, build_time: String)
    case class CurrentTime(current_time:String)
    case class UserAuth(message: String, token: String)
    case class QueueDetails(name:String,details:String,submitted_from:String,execution_time:String)
    case class EtlJobStatus(id: String, name: String, start_time: String, `type`: String, status: String)
    case class Props(key: String, value: String)
    case class CredentialsArgs(name: String, `type`: Creds, value: List[Props])
    case class Job(
                    name: String, props: Map[String,String], schedule: String, nextSchedule: String, schduleRemainingTime: String ,
                    failed: Long, success: Long, is_active:Boolean, last_run_time: Long, last_run_description: String
                  )
    sealed trait Creds
    object Creds {
      case object AWS extends Creds
      case object JDBC extends Creds
    }
    case class UserArgs(user_name: String, password: String)
  }
}
