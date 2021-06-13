package etlflow

import io.circe.generic.semiauto.deriveDecoder
import cron4s.CronExpr

package object schema {

  sealed trait Credential
  object Credential {
    final case class GCP(service_account_key_path: String, project_id: String = "") extends Credential {
      override def toString: String = "****service_account_key_path****"
    }
    final case class AWS(access_key: String, secret_key: String) extends Credential {
      override def toString: String = "****access_key****secret_key****"
    }
    final case class JDBC(url: String, user: String, password: String, driver: String) extends Credential {
      override def toString: String = s"JDBC with url => $url"
    }
    final case class REDIS(host_name: String, password: Option[String] = None, port: Int = 6379) extends Credential {
      override def toString: String = s"REDIS with url $host_name and port $port"
    }
    final case class SMTP(port: String, host: String, user:String, password:String, transport_protocol:String = "smtp", starttls_enable:String = "true", smtp_auth:String = "true") extends Credential {
      override def toString: String = s"SMTP with host  => $host and user => $user"
    }

    object AWS {
      implicit val AwsDecoder = deriveDecoder[AWS]
    }

    object JDBC {
      implicit val JdbcDecoder = deriveDecoder[JDBC]
    }
  }

  case class DbStepRunArgs(job_run_id: String)
  case class DbJobRunArgs(
                           jobRunId: Option[String] = None, jobName: Option[String] = None, startTime: Option[java.time.LocalDate] = None,
                           endTime: Option[java.time.LocalDate] = None, filter: Option[String] = None, limit: Long, offset: Long
                         )
  case class JobLogsArgs(filter: Option[Double] = None, limit:Option[Long] = None)
  case class GetCredential(name: String, `type`: String, valid_from: String)
  case class JobLogs(job_name: String,  success: String, failed: String)
  case class EtlJobStateArgs(name: String, state: Boolean)

  sealed trait Creds
  object Creds {
    case object AWS extends Creds
    case object JDBC extends Creds
  }
  case class Props(key: String, value: String)
  case class CredentialsArgs(name: String, `type`: Creds, value: List[Props])
  case class Credentials(name: String, `type`: String, value: String)
  case class JobRun(job_run_id: String,job_name: String,properties: String,state: String,start_time: String,elapsed_time: String,job_type: String,is_master:String)
  case class StepRun(job_run_id: String,step_name: String,properties: String,state: String,start_time: String,elapsed_time:String,step_type:String,step_run_id:String)
  case class Job(
                  name: String, props: Map[String,String], schedule: Option[CronExpr], nextSchedule: String, schduleRemainingTime: String ,
                  failed: Long, success: Long, is_active:Boolean, last_run_time: Long, last_run_description: String
                )
  case class EtlJob(name: String, props: Map[String,String])

}
