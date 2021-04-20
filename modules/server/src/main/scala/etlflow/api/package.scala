package etlflow

import cron4s.CronExpr
import etlflow.jdbc.DBServerEnv
import zio.{Has, RIO}

package object api {

  type APIEnv = Has[Service]
  type ExecutorEnv = DBServerEnv with JobEnv
  type ServerEnv = APIEnv with ExecutorEnv
  type EtlFlowTask[A] = RIO[ServerEnv, A]

  object Schema {
    // API Arguments
    sealed trait Creds
    object Creds {
      case object AWS extends Creds
      case object JDBC extends Creds
    }
    case class Props(key: String, value: String)
    case class EtlJobStateArgs(name: String, state: Boolean)
    case class EtlJobArgs(name: String, props: Option[List[Props]] = None)
    case class UserArgs(user_name: String, password: String)
    case class DbJobRunArgs(
       jobRunId: Option[String] = None, jobName: Option[String] = None, startTime: Option[java.time.LocalDate] = None,
       endTime: Option[java.time.LocalDate] = None, filter: Option[String] = None, limit: Long, offset: Long
     )
    case class DbStepRunArgs(job_run_id: String)
    case class CronJobArgs(job_name: String, schedule: CronExpr)
    case class CredentialsArgs(name: String, `type`: Creds, value: List[Props])
    case class JobLogsArgs(filter: Option[Double] = None, limit:Option[Long] = None)

    // API Results
    case class EtlJob(name: String, props: Map[String,String])
    case class EtlJobStatus(name: String, status: String, props: Map[String,String])
    case class EtlFlowMetrics(active_jobs: Int, active_subscribers: Int, etl_jobs: Int, cron_jobs: Int, build_time: String)
    case class CurrentTime(current_time:String)
    case class UserAuth(message: String, token: String)
    case class GetCredential(name: String, `type`: String, valid_from: String)
    case class Credentials(name: String, `type`: String, value: String)
    case class CacheInfo(name:String,hitCount:Long,hitRate:Double,size:Long,missCount:Long,missRate:Double,requestCount:Long,data: Map[String,String])
    case class CacheDetails(name:String,details:Map[String,String])
    case class QueueDetails(name:String,details:String,submitted_from:String,execution_time:String)
    case class JobLogs(job_name: String,  success: String, failed: String)
    case class Job(
      name: String, props: Map[String,String], schedule: Option[CronExpr], nextSchedule: String, schduleRemainingTime: String ,
      failed: Long, success: Long, is_active:Boolean, max_active_runs: Int, job_deploy_mode: String, last_run_time: Long, last_run_description: String
    )
  }
}
