
import etlflow.crypto.CryptoEnv
import etlflow.db.DBEnv
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlStep
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.log.{ConsoleEnv, LoggerEnv, SlackEnv}
import etlflow.schema.{Executor, LoggingLevel}
import etlflow.utils.EtlflowError.EtlJobException
import io.circe.Encoder
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Tag, ZIO}

package object etlflow {

  type CoreEnv = DBEnv with JsonEnv with CryptoEnv with LoggerEnv with Blocking with Clock
  type JobEnv = CoreEnv with SlackEnv with ConsoleEnv
  type EJPMType = EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]

  trait EtlJobProps extends Product

  abstract class EtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]](implicit tag_EJ: Tag[EJ], tag_EJP: Tag[EJP], encoder: Encoder[EJP]) {
    val job_description: String               = ""
    val job_schedule: String                  = ""
    val job_max_active_runs: Int              = 10
    val job_deploy_mode: Executor             = Executor.LOCAL
    val job_retries: Int                      = 0
    val job_retry_delay_in_minutes: Int       = 0
    val job_enable_db_logging: Boolean        = true
    val job_send_slack_notification: Boolean  = false
    val job_notification_level: LoggingLevel  = LoggingLevel.INFO

    final val job_name: String                = tag_EJ.tag.longName
    final val job_props_name: String          = tag_EJP.tag.longName

    def getActualProperties(job_properties: Map[String, String]): EJP

    final def etlJob(job_properties: Map[String, String]): EJ = {
      val props = getActualProperties(job_properties)
      tag_EJ.closestClass.getConstructor(tag_EJP.closestClass).newInstance(props).asInstanceOf[EJ]
    }

    final def getActualPropertiesAsJson(job_properties: Map[String, String]): ZIO[JsonEnv, Throwable, String] = {
      JsonApi.convertToString(getActualProperties(job_properties), List.empty)
    }

    final def getProps: Map[String,String] = Map(
      "job_name" -> job_name,
      "job_props_name" -> job_props_name,
      "job_description" -> job_description,
      "job_schedule" -> job_schedule,
      "job_deploy_mode" -> job_deploy_mode.toString,
      "job_max_active_runs" -> job_max_active_runs.toString,
      "job_retries" -> job_retries.toString,
      "job_retry_delay_in_minutes" -> job_retry_delay_in_minutes.toString,
      "job_enable_db_logging" -> job_enable_db_logging.toString,
      "job_send_slack_notification" -> job_send_slack_notification.toString,
      "job_notification_level" -> job_notification_level.toString
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
