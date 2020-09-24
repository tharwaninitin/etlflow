import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlStep
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{Executor, LoggingLevel}

package object etlflow {

  case class EtlJobException(msg : String) extends RuntimeException(msg)
  case class EtlJobNotFoundException(msg : String) extends RuntimeException(msg)

  case class LoggerResource(db: Option[DbLogManager], slack: Option[SlackLogManager])

  abstract class EtlJobName[+EJP <: EtlJobProps] {
    def getActualProperties(job_properties: Map[String, String]): EJP
    def etlJob(job_properties: Map[String, String]): EtlJob[EJP]
  }

  trait EtlJobSchema extends Product

  trait EtlJobProps {
    val job_run_id: String                    = java.util.UUID.randomUUID.toString
    val job_description: String               = ""
    val job_enable_db_logging: Boolean        = true
    val job_send_slack_notification: Boolean  = false
    val job_notification_level: LoggingLevel  = LoggingLevel.INFO //info or debug
    val job_schedule: String                  = ""
    val job_max_active_runs: Int              = 10
    val job_deploy_mode: Executor             = Executor.LOCAL
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
