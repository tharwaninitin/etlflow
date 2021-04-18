import doobie.hikari.HikariTransactor
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlStep
import etlflow.log.{DbJobLogger, DbStepLogger, SlackLogger}
import etlflow.utils.{Executor, LoggingLevel}
import zio.{Has, Task, ZEnv}

import scala.reflect.ClassTag

package object etlflow {

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
  }

  case class EtlJobException(msg : String) extends RuntimeException(msg)
  case class EtlJobNotFoundException(msg : String) extends RuntimeException(msg)
  case class StepLogger(db: Option[DbStepLogger], slack: Option[SlackLogger])
  case class JobLogger(db: Option[DbJobLogger], slack: Option[SlackLogger])
  type StepEnv = Has[StepLogger] with ZEnv
  type TransactorEnv = Has[HikariTransactor[Task]]
  trait EtlJobSchema extends Product
  trait EtlJobProps {}
  type EJPMType = EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]

  abstract class EtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]](implicit tag_EJ: ClassTag[EJ], tag_EJP: ClassTag[EJP]) {
    val job_description: String         = ""
    val job_schedule: String            = ""
    val job_max_active_runs: Int        = 10
    val job_deploy_mode: Executor       = Executor.LOCAL
    val job_retries: Int                = 0
    val job_retry_delay_in_minutes: Int = 0
    val job_enable_db_logging: Boolean        = true
    val job_send_slack_notification: Boolean  = false
    val job_notification_level: LoggingLevel  = LoggingLevel.INFO //info or debug

    final val job_name: String          = tag_EJ.toString
    final val job_props_name: String    = tag_EJP.toString

    def getActualProperties(job_properties: Map[String, String]): EJP
    final def etlJob(job_properties: Map[String, String]): EJ = {
      // https://stackoverflow.com/questions/46798242/scala-create-instance-by-type-parameter
      val props = getActualProperties(job_properties)
      tag_EJ.runtimeClass.getConstructor(tag_EJP.runtimeClass).newInstance(props).asInstanceOf[EJ]
    }
    final def getProps: Map[String,Any] = Map(
        "job_name" -> job_name,
        "job_props_name" -> job_props_name,
        "job_description" -> job_description,
        "job_schedule" -> job_schedule,
        "job_deploy_mode" -> job_deploy_mode,
        "job_max_active_runs" -> job_max_active_runs,
        "job_retries" -> job_retries,
        "job_retry_delay_in_minutes" -> job_retry_delay_in_minutes,
        "job_enable_db_logging" -> job_enable_db_logging,
        "job_send_slack_notification" -> job_send_slack_notification,
        "job_notification_level" -> job_notification_level
      )
  }

  object EtlStepList {
    def apply(args: EtlStep[Unit,Unit]*): List[EtlStep[Unit,Unit]] = {
      val seq = List(args:_*)
      if (seq.map(x => x.name).distinct.length == seq.map(x => x.name).length)
        seq
      else throw EtlJobException(s"Duplicate step name detected from ${seq.map(x => x.name)}")
    }
  }
}
